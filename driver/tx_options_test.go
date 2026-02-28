package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateTxOptions_Default(t *testing.T) {
	opts := driver.TxOptions{}
	err := ValidateTxOptions(opts)
	assert.NoError(t, err)
}

func TestValidateTxOptions_LevelDefault(t *testing.T) {
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelDefault)}
	err := ValidateTxOptions(opts)
	assert.NoError(t, err)
}

func TestValidateTxOptions_LevelSerializable(t *testing.T) {
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelSerializable)}
	err := ValidateTxOptions(opts)
	assert.NoError(t, err)
}

func TestValidateTxOptions_LevelReadUncommitted(t *testing.T) {
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelReadUncommitted)}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var isoErr ErrIsolationLevelNotSupported
	assert.True(t, errors.As(err, &isoErr))
	assert.Equal(t, sql.LevelReadUncommitted, isoErr.Level)
	assert.Contains(t, err.Error(), "Read Uncommitted")
}

func TestValidateTxOptions_LevelReadCommitted(t *testing.T) {
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelReadCommitted)}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var isoErr ErrIsolationLevelNotSupported
	assert.True(t, errors.As(err, &isoErr))
	assert.Equal(t, sql.LevelReadCommitted, isoErr.Level)
}

func TestValidateTxOptions_LevelRepeatableRead(t *testing.T) {
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelRepeatableRead)}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var isoErr ErrIsolationLevelNotSupported
	assert.True(t, errors.As(err, &isoErr))
	assert.Equal(t, sql.LevelRepeatableRead, isoErr.Level)
}

func TestValidateTxOptions_LevelSnapshot(t *testing.T) {
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelSnapshot)}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var isoErr ErrIsolationLevelNotSupported
	assert.True(t, errors.As(err, &isoErr))
	assert.Equal(t, sql.LevelSnapshot, isoErr.Level)
}

func TestValidateTxOptions_LevelLinearizable(t *testing.T) {
	opts := driver.TxOptions{Isolation: driver.IsolationLevel(sql.LevelLinearizable)}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var isoErr ErrIsolationLevelNotSupported
	assert.True(t, errors.As(err, &isoErr))
	assert.Equal(t, sql.LevelLinearizable, isoErr.Level)
}

func TestValidateTxOptions_ReadOnly(t *testing.T) {
	opts := driver.TxOptions{ReadOnly: true}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var roErr ErrReadOnlyNotSupported
	assert.True(t, errors.As(err, &roErr))
	assert.Contains(t, err.Error(), "read-only")
}

func TestValidateTxOptions_ReadOnlyWithSerializable(t *testing.T) {
	opts := driver.TxOptions{
		Isolation: driver.IsolationLevel(sql.LevelSerializable),
		ReadOnly:  true,
	}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var roErr ErrReadOnlyNotSupported
	assert.True(t, errors.As(err, &roErr))
}

func TestValidateTxOptions_UnsupportedIsolationCheckedFirst(t *testing.T) {
	opts := driver.TxOptions{
		Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
		ReadOnly:  true,
	}
	err := ValidateTxOptions(opts)

	assert.Error(t, err)
	var isoErr ErrIsolationLevelNotSupported
	assert.True(t, errors.As(err, &isoErr))
}
