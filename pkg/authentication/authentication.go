package authentication

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wundergraph/wundergraph/pkg/pool"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/cespare/xxhash"
	"github.com/dgraph-io/ristretto"
	"github.com/golang-jwt/jwt/v4"
	"github.com/gorilla/csrf"
	"github.com/gorilla/securecookie"
	"go.uber.org/zap"

	"github.com/wundergraph/wundergraph/pkg/customhttpclient"
	"github.com/wundergraph/wundergraph/pkg/jsonpath"
	"github.com/wundergraph/wundergraph/pkg/loadvariable"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
)

func init() {
	gob.Register(User{})
	gob.Register(map[string]interface{}(nil))
}

type DeniedError string

func (e DeniedError) Error() string { return fmt.Sprintf("access denied: %s", string(e)) }

type UserLoader struct {
	log               *zap.Logger
	s                 *securecookie.SecureCookie
	cache             *ristretto.Cache
	client            *http.Client
	userLoadConfigs   []*UserLoadConfig
	hooks             Hooks
	authRequiredFuncs map[string]func(*http.Request) bool
}

type UserLoadConfig struct {
	jwks             *keyfunc.JWKS
	userInfoEndpoint string
	cacheTtlSeconds  int
	issuer           string
}

// Keyfunc returns a function for retrieving a token key from the
// UserLoadConfig's key set if there are any keys. Otherwise, it
// returns nil.
func (cfg *UserLoadConfig) Keyfunc() jwt.Keyfunc {
	if cfg != nil && cfg.jwks != nil && cfg.jwks.Len() > 0 {
		return cfg.jwks.Keyfunc
	}
	return nil
}

func (u *UserLoader) parseClaims(r io.Reader) (*Claims, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	// Deserialize twice to obtain raw claims
	var claims Claims
	if err = json.Unmarshal(data, &claims); err != nil {
		return nil, err
	}
	if claims.Subject == "" {
		return nil, errors.New("cannot fetch subject from claims")
	}
	if err = json.Unmarshal(data, &claims.Raw); err != nil {
		return nil, err
	}
	return &claims, nil
}

// Load user from token. token is always non nil and contains at least a non-empty token.Raw
// but it might not be validated if we have no key functions
func (u *UserLoader) userFromToken(ctx context.Context, token *jwt.Token, cfg *UserLoadConfig, user *User, revalidate bool) error {
	cacheKey := token.Raw
	if !revalidate {
		fromCache, exists := u.cache.Get(cacheKey)
		if exists {
			*user = fromCache.(User)
			u.log.Debug("user loaded from cache",
				zap.String("sub", user.UserID),
			)
			return nil
		}
	}

	var claims *Claims
	if cfg.userInfoEndpoint != "" {
		// Retrieve claims from userInfoEndpoint
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.userInfoEndpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.Raw))
		// Prevent infinite loops when the userInfo endpoint is also an operation
		customhttpclient.SetTag(req, customhttpclient.RequestTagUserInfo)
		res, err := u.client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		claims, err = u.parseClaims(res.Body)
		if err != nil {
			return err
		}
	} else {
		// Parse claims from token
		encoded, err := json.Marshal(token.Claims)
		if err != nil {
			return err
		}
		claims, err = u.parseClaims(bytes.NewReader(encoded))
		if err != nil {
			return err
		}
	}
	issuer := cfg.issuer
	if issuer == "" {
		issuer = claims.Issuer
	}
	tempUser := claims.ToUser()
	tempUser.ProviderName = "token"
	tempUser.ProviderID = issuer
	tempUser.AccessToken = tryParseJWT(token.Raw)
	tempUser.RawAccessToken = token.Raw
	if err := u.hooks.PostAuthentication(ctx, tempUser); err != nil {
		return err
	}
	tempUser, err := u.hooks.MutatingPostAuthentication(ctx, tempUser)
	if err != nil {
		return err
	}
	if revalidate {
		tempUser, err = u.hooks.RevalidateAuthentication(ctx, tempUser)
		if err != nil {
			return err
		}
	}
	*user = *tempUser
	if cfg.cacheTtlSeconds > 0 {
		u.cache.SetWithTTL(cacheKey, *user, 1, time.Second*time.Duration(cfg.cacheTtlSeconds))
	}
	return nil
}

// User holds user data for non public APIs (backend and hooks). Before exposing
// a User publicly, always call User.ToPublic().
//
// XXX: Keep in sync with the TS side (wellKnownClaimField, type User, type WunderGraphUser)
type User struct {
	ProviderName      string `json:"provider,omitempty"`
	ProviderID        string `json:"providerId,omitempty"`
	UserID            string `json:"userId,omitempty"`
	Name              string `json:"name,omitempty"`
	FirstName         string `json:"firstName,omitempty"`
	LastName          string `json:"lastName,omitempty"`
	MiddleName        string `json:"middleName,omitempty"`
	NickName          string `json:"nickName,omitempty"`
	PreferredUsername string `json:"preferredUsername,omitempty"`
	Profile           string `json:"profile,omitempty"`
	Picture           string `json:"picture,omitempty"`
	Website           string `json:"website,omitempty"`
	Email             string `json:"email,omitempty"`
	EmailVerified     bool   `json:"emailVerified,omitempty"`
	Gender            string `json:"gender,omitempty"`
	BirthDate         string `json:"birthDate,omitempty"`
	ZoneInfo          string `json:"zoneInfo,omitempty"`
	Locale            string `json:"locale,omitempty"`
	Location          string `json:"location,omitempty"`

	CustomClaims     map[string]interface{} `json:"customClaims,omitempty"`
	CustomAttributes []string               `json:"customAttributes,omitempty"`
	Roles            []string               `json:"roles"`
	/* Internal fields */
	ExpiresAt      time.Time       `json:"-"`
	ETag           string          `json:"etag,omitempty"`
	FromCookie     bool            `json:"fromCookie,omitempty"`
	AccessToken    json.RawMessage `json:"accessToken,omitempty"`
	RawAccessToken string          `json:"rawAccessToken,omitempty"`
	IdToken        json.RawMessage `json:"idToken,omitempty"`
	RawIDToken     string          `json:"rawIdToken,omitempty"`
}

// ToPublic returns a copy of the User with fields non intended for public consumption erased. If publicClaims
// is non-empty, only fields listed in it are included. Each public claim must be either a well known claim
// (as in the WG_CLAIM enum) or a JSON path to a custom claim.
func (u *User) ToPublic(publicClaims []string) *User {
	if len(publicClaims) == 0 {
		return u
	}
	cpy := &User{
		CustomAttributes: u.CustomAttributes,
		Roles:            u.Roles,
	}
	for _, claim := range publicClaims {
		if cpy.copyWellKnownClaim(claim, u) {
			continue
		}
		keys := strings.Split(claim, ".")
		value := jsonpath.GetKeys(u.CustomClaims, keys...)
		if value != nil {
			cpy.CustomClaims = jsonpath.SetKeys(cpy.CustomClaims, value, keys...)
		}
	}
	return cpy
}

func (u *User) copyWellKnownClaim(claim string, from *User) bool {
	// XXX: Keep this in sync with WG_CLAIM
	switch claim {
	case wgpb.ClaimType_ISSUER.String(), "PROVIDER":
		u.ProviderID = from.ProviderID
	case wgpb.ClaimType_SUBJECT.String(), "USERID":
		u.UserID = from.UserID
	case wgpb.ClaimType_NAME.String():
		u.Name = from.Name
	case wgpb.ClaimType_GIVEN_NAME.String():
		u.FirstName = from.FirstName
	case wgpb.ClaimType_FAMILY_NAME.String():
		u.LastName = from.LastName
	case wgpb.ClaimType_MIDDLE_NAME.String():
		u.MiddleName = from.MiddleName
	case wgpb.ClaimType_NICKNAME.String():
		u.NickName = from.NickName
	case wgpb.ClaimType_PREFERRED_USERNAME.String():
		u.PreferredUsername = from.PreferredUsername
	case wgpb.ClaimType_PROFILE.String():
		u.Profile = from.Profile
	case wgpb.ClaimType_PICTURE.String():
		u.Picture = from.Picture
	case wgpb.ClaimType_WEBSITE.String():
		u.Website = from.Website
	case wgpb.ClaimType_EMAIL.String():
		u.Email = from.Email
	case wgpb.ClaimType_EMAIL_VERIFIED.String():
		u.EmailVerified = from.EmailVerified
	case wgpb.ClaimType_GENDER.String():
		u.Gender = from.Gender
	case wgpb.ClaimType_BIRTH_DATE.String():
		u.BirthDate = from.BirthDate
	case wgpb.ClaimType_ZONE_INFO.String():
		u.ZoneInfo = from.ZoneInfo
	case wgpb.ClaimType_LOCALE.String():
		u.Locale = from.Locale
	case wgpb.ClaimType_LOCATION.String():
		u.Location = from.Location
	default:
		return false
	}
	return true
}

func (u *User) setCookieExpires(cookie *http.Cookie) {
	if !u.ExpiresAt.IsZero() {
		cookie.Expires = u.ExpiresAt
		cookie.MaxAge = 0
	}
}

func (u *User) Save(s *securecookie.SecureCookie, w http.ResponseWriter, r *http.Request, domain string, insecureCookies bool) error {

	rawIdToken := u.RawIDToken

	// we remove these from the cookie to save space
	u.IdToken = nil
	u.AccessToken = nil
	u.RawAccessToken = ""
	u.RawIDToken = ""

	hash := xxhash.New()
	err := gob.NewEncoder(hash).Encode(*u)
	if err != nil {
		return err
	}

	u.ETag = fmt.Sprintf("W/\"%d\"", hash.Sum64())

	encoded, err := s.Encode("user", *u)
	if err != nil {
		return err
	}

	cookie := &http.Cookie{
		Name:     "user",
		Value:    encoded,
		Path:     "/",
		Domain:   removeSubdomain(sanitizeDomain(domain)),
		MaxAge:   int((time.Hour * 24 * 30).Seconds()),
		Secure:   !insecureCookies,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}

	u.setCookieExpires(cookie)
	http.SetCookie(w, cookie)

	encoded, err = s.Encode("id", rawIdToken)
	if err != nil {
		return err
	}

	cookie = &http.Cookie{
		Name:     "id",
		Value:    encoded,
		Path:     "/",
		Domain:   removeSubdomain(sanitizeDomain(domain)),
		MaxAge:   int((time.Hour * 24 * 30).Seconds()),
		Secure:   !insecureCookies,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}

	u.setCookieExpires(cookie)
	http.SetCookie(w, cookie)

	return nil
}

func (u *User) Load(loader *UserLoader, r *http.Request) error {
	authorizationHeader := r.Header.Get("Authorization")
	// If the request is tagged as an attempt to load the userInfo for a token, don't
	// do anything. Otherwise setting an operation as the endPoint causes an infinite
	// loop.
	if loader.userLoadConfigs != nil && authorizationHeader != "" && customhttpclient.Tag(r) != customhttpclient.RequestTagUserInfo {
		if !strings.HasPrefix(authorizationHeader, "Bearer ") {
			return fmt.Errorf("invalid authorization Header")
		}
		tokenString := strings.TrimPrefix(authorizationHeader, "Bearer ")
		revalidate := r.URL.Query().Has("revalidate")
		lastConfigIndex := len(loader.userLoadConfigs) - 1
		ctx := context.WithValue(r.Context(), pool.ClientRequestKey, r)
		var deniedError DeniedError
		for i, config := range loader.userLoadConfigs {
			keyFunc := config.Keyfunc()
			token, err := jwt.Parse(tokenString, keyFunc)
			// If we have a Keyfunc, enforce a valid token. Otherwise fallback
			// to loader.userFromToken
			if token == nil {
				token = &jwt.Token{
					Raw:   tokenString,
					Valid: false,
				}
			}
			isLastConfig := i == lastConfigIndex
			if keyFunc != nil && isLastConfig {
				if err != nil {
					loader.log.Warn("could not parse token", zap.String("token", tokenString),
						zap.Time("timeFunc", jwt.TimeFunc()), zap.Error(err))
					continue
				}
				if token != nil && !token.Valid {
					loader.log.Warn("token is invalid", zap.Any("token", token))
					continue
				}
			}

			if err = loader.userFromToken(ctx, token, config, u, revalidate); err == nil || errors.As(err, &deniedError) {
				return err
			}
			err = nil
		}
	}

	cookie, err := r.Cookie("user")
	if err != nil {
		return err
	}
	err = loader.s.Decode("user", cookie.Value, u)
	if err == nil {
		u.FromCookie = true
	}
	cookie, err = r.Cookie("id")
	if err != nil {
		return err
	}
	err = loader.s.Decode("id", cookie.Value, &u.RawIDToken)
	u.IdToken = tryParseJWT(u.RawIDToken)
	return err
}

func bearerTokenToJSON(token string) ([]byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func tryParseJWT(token string) []byte {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	return payload
}

func ValidateRedirectURIQueryParameter(matchString, matchRegex []string) func(handler http.Handler) http.Handler {
	validator := NewRedirectValidator(matchString, matchRegex)
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, authorized := validator.GetValidatedRedirectURI(r)
			if authorized {
				handler.ServeHTTP(w, r)
				return
			}
			http.Error(w, "invalid redirect uri", http.StatusBadRequest)
		})
	}
}

type RedirectURIValidator struct {
	stringMatchers []string
	regexMatchers  []*regexp.Regexp
}

func NewRedirectValidator(matchString, matchRegex []string) *RedirectURIValidator {
	regexMatchers := make([]*regexp.Regexp, 0, len(matchRegex))

	for _, uri := range matchRegex {
		matcher, err := regexp.Compile(uri)
		if err == nil {
			regexMatchers = append(regexMatchers, matcher)
		}
	}

	stringMatchers := make([]string, 0, len(matchString)*2)
	for _, str := range matchString {
		stringMatchers = append(stringMatchers, str)
		if strings.HasSuffix(str, "/") {
			stringMatchers = append(stringMatchers, strings.TrimSuffix(str, "/"))
		} else {
			stringMatchers = append(stringMatchers, str+"/")
		}
	}

	return &RedirectURIValidator{
		stringMatchers: stringMatchers,
		regexMatchers:  regexMatchers,
	}
}

func (v *RedirectURIValidator) GetValidatedRedirectURI(r *http.Request) (redirectURI string, authorized bool) {
	redirectURI = r.URL.Query().Get("redirect_uri")
	if redirectURI == "" {
		redirectURICookie, err := r.Cookie("success_redirect_uri")
		if err != nil || redirectURICookie == nil {
			return "", false
		}
		redirectURI = redirectURICookie.Value
	}
	if redirectURI == "" {
		return "", false
	}
	for i := range v.stringMatchers {
		if v.stringMatchers[i] == redirectURI {
			return redirectURI, true
		}
	}
	for _, matcher := range v.regexMatchers {
		if matcher.MatchString(redirectURI) {
			return redirectURI, true
		}
	}
	return redirectURI, false
}

func RedirectAlreadyAuthenticatedUsers(matchString, matchRegex []string) func(handler http.Handler) http.Handler {
	validator := NewRedirectValidator(matchString, matchRegex)
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if user := UserFromContext(r.Context()); user != nil {
				redirectURI, authorized := validator.GetValidatedRedirectURI(r)
				if authorized {
					http.Redirect(w, r, redirectURI, http.StatusTemporaryRedirect)
					return
				}
			}
			handler.ServeHTTP(w, r)
		})
	}
}

const (
	HeaderRbacRequireMatchAll = "x-rbac-requireMatchAll"
	HeaderRbacRequireMatchAny = "x-rbac-requireMatchAny"
	HeaderRbacDenyMatchAll    = "x-rbac-denyMatchAll"
	HeaderRbacDenyMatchAny    = "x-rbac-denyMatchAny"
)

type RBACEnforcer struct {
	authorizationConfig *wgpb.OperationAuthorizationConfig
}

func NewRBACEnforcer(operation *wgpb.Operation) *RBACEnforcer {
	return &RBACEnforcer{authorizationConfig: operation.AuthorizationConfig}
}

func (e *RBACEnforcer) Enforce(r *http.Request) (proceed bool) {
	if ok := e.enforceRequireMatchAll(r); !ok {
		return false
	}
	if ok := e.enforceRequireMatchAny(r); !ok {
		return false
	}
	if ok := e.enforceDenyMatchAll(r); !ok {
		return false
	}
	if ok := e.enforceDenyMatchAny(r); !ok {
		return false
	}
	return true
}

func (e *RBACEnforcer) hasRoleConfig() bool {
	return e.authorizationConfig != nil && e.authorizationConfig.RoleConfig != nil
}

func (e *RBACEnforcer) enforceRequireMatchAll(r *http.Request) bool {
	var requireMatchAll []string
	if value, ok := r.Header[HeaderRbacRequireMatchAll]; ok {
		requireMatchAll = value
	} else if e.hasRoleConfig() {
		requireMatchAll = e.authorizationConfig.RoleConfig.RequireMatchAll
	}
	if len(requireMatchAll) == 0 {
		return true
	}
	user := UserFromContext(r.Context())
	if user == nil {
		return false
	}
	for _, match := range requireMatchAll {
		if contains := e.containsOne(user.Roles, match); !contains {
			return false
		}
	}
	return true
}

func (e *RBACEnforcer) enforceRequireMatchAny(r *http.Request) bool {
	var requireMatchAny []string
	if value, ok := r.Header[HeaderRbacRequireMatchAny]; ok {
		requireMatchAny = value
	} else if e.hasRoleConfig() {
		requireMatchAny = e.authorizationConfig.RoleConfig.RequireMatchAny
	}
	if len(requireMatchAny) == 0 {
		return true
	}
	user := UserFromContext(r.Context())
	if user == nil {
		return false
	}
	for _, match := range requireMatchAny {
		if contains := e.containsOne(user.Roles, match); contains {
			return true
		}
	}
	return false
}

func (e *RBACEnforcer) enforceDenyMatchAll(r *http.Request) bool {
	var denyMatchAll []string
	if value, ok := r.Header[HeaderRbacDenyMatchAll]; ok {
		denyMatchAll = value
	} else if e.hasRoleConfig() {
		denyMatchAll = e.authorizationConfig.RoleConfig.DenyMatchAll
	}
	if len(denyMatchAll) == 0 {
		return true
	}
	user := UserFromContext(r.Context())
	if user == nil {
		return false
	}
	for _, match := range denyMatchAll {
		if contains := e.containsOne(user.Roles, match); !contains {
			return true
		}
	}
	return false
}

func (e *RBACEnforcer) enforceDenyMatchAny(r *http.Request) bool {
	var denyMatchAny []string
	if value, ok := r.Header[HeaderRbacDenyMatchAny]; ok {
		denyMatchAny = value
	} else if e.hasRoleConfig() {
		denyMatchAny = e.authorizationConfig.RoleConfig.DenyMatchAny
	}
	if len(denyMatchAny) == 0 {
		return true
	}
	user := UserFromContext(r.Context())
	if user == nil {
		return false
	}
	for _, match := range denyMatchAny {
		if contains := e.containsOne(user.Roles, match); contains {
			return false
		}
	}
	return true
}

func (e *RBACEnforcer) containsOne(slice []string, one string) bool {
	for i := range slice {
		if slice[i] == one {
			return true
		}
	}
	return false
}

type LoadUserConfig struct {
	Log           *zap.Logger
	Cookie        *securecookie.SecureCookie
	JwksProviders []*wgpb.JwksAuthProvider
	Hooks         Hooks
}

func NewLoadUserMw(config LoadUserConfig) (map[string]func(*http.Request) bool, *ristretto.Cache, func(handler http.Handler) http.Handler) {

	var (
		jwkConfigs    []*UserLoadConfig
		jwkHttpClient = &http.Client{Timeout: 5 * time.Second}
	)

	if config.JwksProviders != nil {
		for _, provider := range config.JwksProviders {
			issuer := loadvariable.String(provider.Issuer)
			providerConfig, err := introspectionOpenIDConnectProvider(issuer, jwkHttpClient, false)
			if err != nil {
				config.Log.Error("jwks introspection failed",
					zap.String("authentication", provider.Id),
					zap.Error(err), zap.String("issuer", issuer))
				continue
			}

			userInfoEndpoint := providerConfig.UserinfoEndpoint
			userInfoURL, err := url.Parse(userInfoEndpoint)
			if err != nil {
				config.Log.Error("jwks userInfo endpoint invalid URL",
					zap.String("authentication", provider.Id),
					zap.Error(err), zap.String("URL", userInfoEndpoint))
				continue
			}
			if jwksURL := providerConfig.JwksUri; jwksURL != "" {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				jwks, err := keyfunc.Get(jwksURL, keyfunc.Options{Ctx: ctx})
				cancel()
				if err != nil {
					config.Log.Error("loading jwks from URL failed",
						zap.String("authentication", provider.Id),
						zap.Error(err), zap.String("URL", jwksURL))
					continue
				}
				jwkConfigs = append(jwkConfigs, &UserLoadConfig{
					jwks:             jwks,
					userInfoEndpoint: userInfoEndpoint,
					cacheTtlSeconds:  int(provider.UserInfoCacheTtlSeconds),
					issuer:           userInfoURL.Host,
				})
				continue
			}
			if js := loadvariable.String(provider.JwksJson); js != "" {
				jwks, err := keyfunc.NewJSON(json.RawMessage(js))
				if err != nil {
					config.Log.Error("loading jwks from JSON failed",
						zap.String("authentication", provider.Id),
						zap.Error(err), zap.String("JSON", js))
					continue
				}
				jwkConfigs = append(jwkConfigs, &UserLoadConfig{
					jwks:             jwks,
					userInfoEndpoint: userInfoEndpoint,
					cacheTtlSeconds:  int(provider.UserInfoCacheTtlSeconds),
					issuer:           userInfoURL.Host,
				})
				continue
			}
			jwkConfigs = append(jwkConfigs, &UserLoadConfig{
				jwks:             keyfunc.NewGiven(map[string]keyfunc.GivenKey{}),
				userInfoEndpoint: userInfoEndpoint,
				cacheTtlSeconds:  int(provider.UserInfoCacheTtlSeconds),
				issuer:           userInfoURL.Host,
			})
		}
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1024 * 1024 / 10,
		MaxCost:     1024 * 1024,
		BufferItems: 64,
	})

	if err != nil {
		config.Log.Error("unable to instantiate user loader cache",
			zap.Error(err),
		)
	}

	authRequiredFuncs := make(map[string]func(*http.Request) bool)
	loader := &UserLoader{
		log:             config.Log,
		userLoadConfigs: jwkConfigs,
		s:               config.Cookie,
		cache:           cache,
		client: &http.Client{
			Timeout: time.Second * 10,
		},
		hooks:             config.Hooks,
		authRequiredFuncs: authRequiredFuncs,
	}

	return authRequiredFuncs, cache, func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var (
				user        User
				deniedError DeniedError
			)
			loadErr := user.Load(loader, r)
			if errors.As(loadErr, &deniedError) {
				if requiredFunc, ok := authRequiredFuncs[r.URL.Path]; ok && (requiredFunc == nil || requiredFunc(r)) {
					http.Error(w, loadErr.Error(), http.StatusUnauthorized)
					return
				}
			}
			if loadErr == nil {
				r = r.WithContext(context.WithValue(r.Context(), "user", &user))
				userBytes, _ := json.Marshal(user)
				r = r.WithContext(context.WithValue(r.Context(), "userBytes", userBytes))
			}
			handler.ServeHTTP(w, r)
		})
	}
}

func UserFromContext(ctx context.Context) *User {
	user := ctx.Value("user")
	if actual, ok := user.(*User); ok {
		return actual
	}
	return nil
}

func UserBytesFromContext(ctx context.Context) []byte {
	user := ctx.Value("userBytes")
	if actual, ok := user.([]byte); ok {
		return actual
	}
	return nil
}

type UserHandler struct {
	Log             *zap.Logger
	Host            string
	InsecureCookies bool
	Hooks           Hooks
	Cookie          *securecookie.SecureCookie
	PublicClaims    []string
}

func (u *UserHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user := UserFromContext(r.Context())
	if user == nil {
		// http.NotFound(w, r)
		http.Error(w, "404 user not found", http.StatusNotFound)
		return
	}

	if user.FromCookie && r.URL.Query().Has("revalidate") {
		var err error
		user, err = u.Hooks.RevalidateAuthentication(r.Context(), user)
		if err != nil {
			u.Log.Error("revalidating authentication", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		err = user.Save(u.Cookie, w, r, u.Host, u.InsecureCookies)
		if err != nil {
			u.Log.Error("RevalidateAuthentication could not save cookie", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if tag := r.Header.Get("If-None-Match"); tag != "" && tag == user.ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if user.ETag != "" {
		w.Header()["ETag"] = []string{user.ETag}
		w.Header().Set("Cache-Control", "private, max-age=0, stale-while-revalidate=60")
	}

	encoder := json.NewEncoder(w)
	if r.Header.Get("Accept") != "application/json" {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(user.ToPublic(u.PublicClaims)); err != nil {
		u.Log.Error("encoding user", zap.Error(err))
	}
}

type CSRFTokenHandler struct{}

func (*CSRFTokenHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := csrf.Token(r)
	_, _ = w.Write([]byte(token))
}

type UserLogoutHandler struct {
	InsecureCookies bool
	OpenIDProviders *OpenIDConnectProviderSet
	Hooks           Hooks
	Log             *zap.Logger
}

func (u *UserLogoutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resetUserCookies(w, r, !u.InsecureCookies)
	user := UserFromContext(r.Context())
	if user == nil {
		return
	}
	if err := u.Hooks.PostLogout(r.Context(), user); err != nil {
		if u.Log != nil {
			u.Log.Error("running postLogout hook", zap.Error(err))
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if strings.ToLower(r.URL.Query().Get("logout_openid_connect_provider")) == "true" {
		if err := u.logoutFromProvider(w, r, user); err != nil {
			if u.Log != nil {
				u.Log.Warn("could not disconnect user from OIDC provider", zap.Error(err))
			}
		}
	}
}

func (u *UserLogoutHandler) logoutFromProvider(w http.ResponseWriter, r *http.Request, user *User) error {
	if user.ProviderName != "oidc" {
		return fmt.Errorf("user provider %q is not OpenIDConnect", user.ProviderName)
	}
	if user.ProviderID == "" {
		return errors.New("user has no provider ID")
	}
	provider, err := u.OpenIDProviders.ByID(user.ProviderID)
	if err != nil {
		return err
	}
	result, err := provider.Disconnect(r.Context(), user)
	if err != nil {
		return err
	}
	if result.RequiresClientCooperation() {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		return enc.Encode(&result)
	}
	return nil
}

type CSRFErrorHandler struct {
	InsecureCookies bool
}

func (u *CSRFErrorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resetUserCookies(w, r, !u.InsecureCookies)
	http.Error(w, "forbidden", http.StatusForbidden)
}

// sanitizeDomain cleans up the host so that it can work on localhost
func sanitizeDomain(domain string) string {
	if !strings.Contains(domain, ":") {
		return domain
	}
	i := strings.Index(domain, ":")
	return domain[:i]
}

func removeSubdomain(domain string) string {
	parts := strings.Split(domain, ".")
	if len(parts) < 3 {
		return domain
	}
	return strings.Join(parts[1:], ".")
}

type CSRFConfig struct {
	Path            string
	InsecureCookies bool
	Secret          []byte
}

func NewCSRFMw(config CSRFConfig) func(handler http.Handler) http.Handler {
	return func(unprotected http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if user := UserFromContext(r.Context()); user != nil && user.FromCookie {
				domain := removeSubdomain(sanitizeDomain(r.Host))
				csrfMiddleware := csrf.Protect(config.Secret,
					csrf.Path("/"),
					csrf.Domain(domain),
					csrf.CookieName("csrf"),
					csrf.RequestHeader("X-CSRF-Token"),
					csrf.HttpOnly(true),
					csrf.Secure(!config.InsecureCookies),
					csrf.SameSite(csrf.SameSiteStrictMode),
					csrf.ErrorHandler(&CSRFErrorHandler{
						InsecureCookies: config.InsecureCookies,
					}),
				)
				csrfMiddleware(unprotected).ServeHTTP(w, r)
				return
			}
			unprotected.ServeHTTP(w, r)
		})
	}
}

func EnsureRequiresAuthentication(operation *wgpb.Operation, handler http.Handler) (http.Handler, bool) {
	if operation.AuthenticationConfig == nil || !operation.AuthenticationConfig.AuthRequired {
		return handler, false
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if UserFromContext(r.Context()) == nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	}), true
}

func resetUserCookies(w http.ResponseWriter, r *http.Request, secure bool) {
	for _, name := range []string{"user", "id"} {
		userCookie := &http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			Domain:   removeSubdomain(sanitizeDomain(r.Host)),
			MaxAge:   -1,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			Secure:   secure,
		}
		http.SetCookie(w, userCookie)
	}
}

func postAuthentication(ctx context.Context, w http.ResponseWriter, r *http.Request, hooks Hooks, user *User, cookie *securecookie.SecureCookie, insecureCookies bool) error {
	if err := hooks.PostAuthentication(ctx, user); err != nil {
		return err
	}
	user, err := hooks.MutatingPostAuthentication(r.Context(), user)
	if err != nil {
		return err
	}
	if err := user.Save(cookie, w, r, r.Host, insecureCookies); err != nil {
		return err
	}
	return nil
}
