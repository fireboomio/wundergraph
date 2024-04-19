# 背景  
此项目是对 [wundergraph](https://github.com/wundergraph/wundergraph) 的扩展, 主要新增以下功能点来满足当前的业务诉求
- 使用 go 代码替换官方 sdk 中使用 ts 生成相关启动文件「config.json, schema.graphql」的逻辑
- 对外提供接口, 调用方可以直接调用相关接口, 重新编译并启动 wundergraph 相关服务
- 添加业务需要的功能「生成 swagger.json、自定义 ts 文件」

# 如何使用
## 1. 使用 demo
```
import (
	"context"
	"fmt"
	"github.com/wundergraph/wundergraph/cli/commands"
	"github.com/wundergraph/wundergraph/cli/commands/fb_gen/types"
	"github.com/wundergraph/wundergraph/pkg/logging"
	"go.uber.org/zap/zapcore"
)

func main(ctx context.Context, logLevel zapcore.Level) {
	// 1. 初始化 cli, 需要全局唯一
	log := logging.New(true, false, logLevel)
	wdgCli := commands.NewFbWdgClient(ctx, log)

	// 2. 编译并启动服务「并发安全」
	req := types.FbWdgReq{}
	wdgCli.GenerateAndStart(req)

	// 3. 获取引擎的状态和编译过程中的错误信息「必须, 如果调用方不处理 status 和 questions 会导致 channel 写满, 服务阻塞」
	go func() {
		for {
			select {
			case status := <-wdgConfig.GetStatus():
				fmt.Println("status: ", status)
			case <-wdgConfig.GetQuestionDone():
				for _, q := range wdgConfig.GetQuestions() {
					fmt.Println(q)
				}
			}
		}
	}()
}
```
## 2. 字段说明
```
// 参考 wundergraph 的 config.ts 文件
type FbWdgReq struct {
	ApplicationName         string                           `json:"application_name"` 
	DataSources             []DataSource                     `json:"data_sources"` // 数据源配置
	Server                  *WunderGraphHooksAndServerConfig `json:"server"` // hooks 相关配置
	Cors                    *wgpb.CorsConfiguration          `json:"cors"`
	Security                *SecurityConfig                  `json:"security"`
	S3UploadProvider        []*wgpb.S3UploadConfiguration    `json:"s3_upload_provider"`
	Authorization           Authorization                    `json:"authorization"`
	OperationsConfiguration *OperationConfig                 `json:"operations_configuration"`
	Authentication          *ApiAuthentication               `json:"authentication"`
	DotGraphQLConfig        *DotGraphQLConfig                `json:"dot_graphQL_config"`
	SDKMap                  map[string]string                `json:"sdk_map"`
	HooksServerURL          string                           `json:"hooks_server_url"`
	GeneratedPath           string                           `json:"generated_path"` // 生成目录 custom_ts
	OperationsPath          string                           `json:"operations_path"` // opertion 所在目录
	FragmentsPath           string                           `json:"fragments_path"`
	SchemaPreFixPath        string                           `json:"schema_pre_fix_path"`
	ConfigJsonPath          string                           `json:"config_json_path"` // 生成 wdg 启动所需的配置文件
	SwaggerPath             string                           `json:"swagger_path"`
	RootPath                string                           `json:"root_path"` // 生成的根目录，db instropect 依赖此目录
	NodeOptions             *wgpb.NodeOptions                `json:"node_options"`
	ServerOptions           *wgpb.ServerOptions              `json:"server_options"`
}

// question 定义
type QuestionModel int

// 此时的声明是对应wundergraph包内的问题
const (
	DatasourceQuestion QuestionModel = iota + 1
	OperationQuestion
	HooksQuestion
	AuthQuestion
	OssQuestion
	OauthQuestion
	InternalQuestion // 系统内容错误,非预期的错误
)

type Question struct {
	Model     QuestionModel `json:"model"` // 模块
	Name      string        `json:"name"`  // 数据源的名称, operation 的名称
	ReasonMsg string        `json:"msg"`   // 具体错误
}
```

# 技术方案
## 流程图
![alt 属性文本](./assets/fb/yuque_diagram.jpg)
## 代码说明
> ---- commands  
------ fb_gen //业务扩展需要的 pkg  
-------- codeGen //生成 custom_ts 目录下需要的文件, 用于启动 hookserver     
-------- common // 通用的方法  
-------- configure // 处理生成 configure.json 所需要的逻辑, 主要包括各种结构体的转换, 参数构造  
-------- graphql // graphql 解析需要的一些通用逻辑  
-------- introspect // 内省, 解析出数据源对应的 graphql  
-------- opertion // 解析 operation 解析, 指令、rbac、auth  
-------- swagger // 生成 swagger 文件  
-------- types // 结构体定义  
------ fb_api.go // 接口定义  
------ fb_api_impl.go // 接口实现, 并发控制, gen 流程调度, 错误信息处理, 启动 server
## 开发说明
  - main 分支是作为和官方 sdk 进行代码同步的分支, fireboom 调用的分支为 [gen](https://codeup.aliyun.com/619f00a99cbfc1b4f0467458/fireboom/wundergraph/tree/gen)
# TODO 
## fireboom 如何获取 wdg 的日志, 能否通过自定义的 log 实现