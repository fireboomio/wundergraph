package database

import "go.uber.org/zap"

const applyMigrations schemaMethod = "applyMigrations"

func NewApplyMigrationsCmd(logger *zap.Logger) *ApplyMigrationsCmdExecute {
	return &ApplyMigrationsCmdExecute{
		Method: applyMigrations,
		logger: logger,
	}
}

type (
	ApplyMigrationsCmdExecute = SchemaCommandExecute[ApplyMigrationsInput, ApplyMigrationsOutput]
	ApplyMigrationsInput      struct {
		// The location of the migrations directory.
		MigrationsDirectoryPath string `json:"migrationsDirectoryPath"`
	}
	ApplyMigrationsOutput struct {
		/// The names of the migrations that were just applied. Empty if no migration was applied.
		AppliedMigrationNames []string `json:"appliedMigrationNames"`
	}
)
