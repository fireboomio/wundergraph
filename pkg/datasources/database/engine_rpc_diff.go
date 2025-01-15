package database

import "go.uber.org/zap"

const diff schemaMethod = "diff"

func NewDiffCmd(logger *zap.Logger) *DiffCmdExecute {
	return &DiffCmdExecute{
		Method: diff,
		logger: logger,
	}
}

type (
	DiffCmdExecute = SchemaCommandExecute[DiffInput, DiffOutput]
	DiffInput      struct {
		// Whether the --exit-code param was passed.
		//
		// If this is set, the engine will return exitCode = 2 in the diffResult in case the diff is
		// non-empty. Other than this, it does not change the behaviour of the command.
		ExitCode bool `json:"exitCode,omitempty"`
		// The source of the schema to consider as a _starting point_.
		From DiffInputTarget `json:"from"`
		// By default, the response will contain a human-readable diff. If you want an
		// executable script, pass the `"script": true` param.
		Script bool `json:"script"`
		// The URL to a live database to use as a shadow database. The schema and data on
		// that database will be wiped during diffing.
		//
		// This is only necessary when one of `from` or `to` is referencing a migrations
		// directory as a source for the schema.
		ShadowDatabaseUrl string `json:"shadowDatabaseUrl"`
		// The source of the schema to consider as a _destination_, or the desired
		// end-state.
		To DiffInputTarget `json:"to"`
	}
	DiffOutput struct {
		/// The exit code that the CLI should return.
		ExitCode uint32 `json:"exitCode"`
	}
	DiffInputTargetAlias DiffInputTarget
	DiffInputTarget      struct {
		// The path to a Prisma schema. The contents of the schema itself will be
		// considered. This source does not need any database connection.
		SchemaDatamodel *SchemaContainer `json:"schemaDatamodel,omitempty"`
		// An empty schema.
		Empty bool `json:"empty,omitempty"`
		// The path to a migrations directory of the shape expected by Prisma Migrate. The
		// migrations will be applied to a **shadow database**, and the resulting schema
		// considered for diffing.
		Migrations *PathContainer `json:"migrations,omitempty"`
		// The path to a Prisma schema. The _datasource url_ will be considered, and the
		// live database it points to introspected for its schema.
		SchemaDatasource *SchemaContainer `json:"schemaDatasource,omitempty"`
		// The url to a live database. Its schema will be considered.
		//
		// This will cause the schema engine to connect to the database and read from it.
		// It will not write.
		URL *UrlContainer `json:"url,omitempty"`
	}
	SchemaContainer struct {
		Schema string `json:"schema"`
	}
	PathContainer struct {
		Path string `json:"path"`
	}
	UrlContainer struct {
		Url string `json:"url"`
	}
)

func (d *DiffInputTarget) MarshalJSON() ([]byte, error) {
	return makeRustEnumJsonBytes(DiffInputTargetAlias(*d))
}
