package database

import "go.uber.org/zap"

const createMigration schemaMethod = "createMigration"

func NewCreateMigrationCmd(logger *zap.Logger) *CreateMigrationCmdExecute {
	return &CreateMigrationCmdExecute{
		Method: createMigration,
		logger: logger,
	}
}

type (
	CreateMigrationCmdExecute = SchemaCommandExecute[CreateMigrationInput, CreateMigrationOutput]
	CreateMigrationInput      struct {
		// If true, always generate a migration, but do not apply.
		Draft bool `json:"draft"`
		// The user-given name for the migration. This will be used for the migration directory.
		MigrationName string `json:"migrationName"`
		// The filesystem path of the migrations directory to use.
		MigrationsDirectoryPath string `json:"migrationsDirectoryPath"`
		// The Prisma schema to use as a target for the generated migration.
		PrismaSchema string `json:"prismaSchema"`
	}
	CreateMigrationOutput struct {
		GeneratedMigrationName string `json:"generatedMigrationName,omitempty"`
	}
)
