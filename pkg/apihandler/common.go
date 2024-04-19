package apihandler

import (
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/astparser"
	"github.com/wundergraph/graphql-go-tools/pkg/asttransform"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/wundergraph/pkg/apicache"
	"github.com/wundergraph/wundergraph/pkg/engineconfigloader"
	"github.com/wundergraph/wundergraph/pkg/wgpb"
	"go.uber.org/zap"
	"os"
)

func Common(loader *engineconfigloader.EngineConfigLoader, api *Api, builder *Builder, internalBuilder *InternalBuilder) error {
	if api.EngineConfiguration == nil {
		return nil
	}
	if api.AuthenticationConfig == nil ||
		api.AuthenticationConfig.Hooks == nil {
		return fmt.Errorf("authentication config missing")
	}

	planConfig, err := loader.Load(api.EngineConfiguration, api.Options.ServerUrl)
	if err != nil {
		return err
	}

	// RenameTo is the correct name for the origin
	// for the downstream (client), we have to reverse the __typename fields
	// this is why Types.RenameTo is assigned to rename.From
	var renameTypeNames []resolve.RenameTypeName
	for _, configuration := range planConfig.Types {
		renameTypeNames = append(renameTypeNames, resolve.RenameTypeName{
			From: []byte(configuration.RenameTo),
			To:   []byte(configuration.TypeName),
		})
	}

	definition, report := astparser.ParseGraphqlDocumentString(api.EngineConfiguration.GraphqlSchema)
	if report.HasErrors() {
		return report
	}
	err = asttransform.MergeDefinitionWithBaseSchema(&definition)
	if err != nil {
		return err
	}

	builder.planConfig = *planConfig
	internalBuilder.planConfig = *planConfig

	builder.renameTypeNames = renameTypeNames
	internalBuilder.renameTypeNames = renameTypeNames

	builder.definition = &definition
	internalBuilder.definition = &definition

	if api.CacheConfig != nil {
		if builder.cache, err = configureCache(api, builder.log); err != nil {
			return err
		}
		internalBuilder.cache = builder.cache
	}

	return nil
}

func configureCache(api *Api, log *zap.Logger) (cache *apicache.CacheStruct, err error) {
	config := api.CacheConfig
	switch config.Kind {
	case wgpb.ApiCacheKind_IN_MEMORY_CACHE:
		log.Debug("configureCache",
			zap.String("primaryHost", api.PrimaryHost),
			zap.String("deploymentID", api.DeploymentId),
			zap.String("cacheKind", config.Kind.String()),
			zap.Int("cacheSize", int(config.InMemoryConfig.MaxSize)),
		)
		cache, err = apicache.NewInMemory(config.InMemoryConfig.MaxSize)
		return
	case wgpb.ApiCacheKind_REDIS_CACHE:
		redisAddr := os.Getenv(config.RedisConfig.RedisUrlEnvVar)
		log.Debug("configureCache",
			zap.String("primaryHost", api.PrimaryHost),
			zap.String("deploymentID", api.DeploymentId),
			zap.String("cacheKind", config.Kind.String()),
			zap.String("envVar", config.RedisConfig.RedisUrlEnvVar),
			zap.String("redisAddr", redisAddr),
		)

		cache, err = apicache.NewRedis(redisAddr, log)
		return
	default:
		log.Debug("configureCache",
			zap.String("primaryHost", api.PrimaryHost),
			zap.String("deploymentID", api.DeploymentId),
			zap.String("cacheKind", config.Kind.String()),
		)
		noCache := &apicache.NoOpCache{}
		cache = &apicache.CacheStruct{Cache: noCache}
		return
	}
}

func getGeneratedVariables(doc *ast.Document) []byte {
	var generatedVariables []byte
	for _, item := range doc.VariableValues {
		if !item.Generated {
			continue
		}
		if generatedVariables == nil {
			generatedVariables = []byte("{}")
		}
		variableName := doc.Input.ByteSliceString(item.Name)
		variableValue, valueType, offset, _ := jsonparser.Get(doc.Input.Variables, variableName)
		if valueType == jsonparser.String {
			variableValue = doc.Input.Variables[offset-len(variableValue)-2 : offset]
		}
		generatedVariables, _ = jsonparser.Set(generatedVariables, variableValue, variableName)
	}
	return generatedVariables
}
