// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"time"
)

type execBindParameter struct {
	Type  string  `json:"type"`
	Value *string `json:"value"`
}

type execRequest struct {
	SQLText    string                       `json:"sqlText"`
	AsyncExec  bool                         `json:"asyncExec"`
	SequenceID uint64                       `json:"sequenceId"`
	IsInternal bool                         `json:"isInternal"`
	Parameters map[string]string            `json:"parameters,omitempty"`
	Bindings   map[string]execBindParameter `json:"bindings,omitempty"`
}
type execResponseRowType struct {
	Name       string `json:"name"`
	ByteLength int64  `json:"byteLength"`
	Length     int64  `json:"length"`
	Type       string `json:"type"`
	Precision  int64  `json:"precision"`
	Scale      int64  `json:"scale"`
	Nullable   bool   `json:"nullable"`
}

type execResponseChunk struct {
	URL              string `json:"url"`
	RowCount         int    `json:"rowCount"`
	UncompressedSize int64  `json:"uncompressedSize"`
	CompressedSize   int64  `json:"compressedSize"`
}

// Struct to hold upload information for a PUT command. Not used?
type uploadInfo struct {
	LocationType   string `json:"locationType"`
	Location       string `json:"location"`
	Path           string `json:"path"`
	Region         string `json:"region"`
	StorageAccount string `json:"storageAccount"`
	Endpoint       string `json:"endpoint"`
	// Missing creds
}

// Struct to hold stage information for a put command
type stageInfo struct {
	LocationType   string `json:"locationType"`
	Location       string `json:"location"`
	Path           string `json:"path"`
	Region         string `json:"region"`
	StorageAccount string `json:"storageAccount"`
	Endpoint       string `json:"endpoint""`
	// Missing creds
}

// All data fields are optional as a way to get around JSON deserialization
type execResponseData struct {
	// succeed query response data
	Parameters         []nameValueParameter  `json:"parameters,omitempty"`
	RowType            []execResponseRowType `json:"rowtype,omitempty"`
	RowSet             [][]*string           `json:"rowset,omitempty"`
	Total              int64                 `json:"total,omitempty"`    // java:long
	Returned           int64                 `json:"returned,omitempty"` // java:long
	QueryID            string                `json:"queryId,omitempty"`
	SQLState           string                `json:"sqlState,omitempty"`
	DatabaseProvider   string                `json:"databaseProvider,omitempty"`
	FinalDatabaseName  string                `json:"finalDatabaseName,omitempty"`
	FinalSchemaName    string                `json:"finalSchemaName,omitempty"`
	FinalWarehouseName string                `json:"finalWarehouseName,omitempty"`
	FinalRoleName      string                `json:"finalRoleName,omitempty"`
	NumberOfBinds      int                   `json:"numberOfBinds,omitempty"`   // java:int
	StatementTypeID    int64                 `json:"statementTypeId,omitempty"` // java:long
	Version            int64                 `json:"version,omitempty"`         // java:long
	Chunks             []execResponseChunk   `json:"chunks,omitempty"`
	Qrmk               string                `json:"qrmk,omitempty"`
	ChunkHeaders       map[string]string     `json:"chunkHeaders,omitempty"`

	// ping pong response data
	GetResultURL      string        `json:"getResultUrl,omitempty"`
	ProgressDesc      string        `json:"progressDesc,omitempty"`
	QueryAbortTimeout time.Duration `json:"queryAbortsAfterSecs,omitempty"`

	// Put / Get response data
	UploadInfo                    uploadInfo `json:"uploadInfo""`
	SrcLocations                  []string   `json:"src_locations"`
	Parallel                      int        `json:"parallel"`
	Kind                          string     `json:"kind"`
	AutoCompress                  bool       `json:"autoCompress"`
	Overwrite                     bool       `json:"overwrite"`
	SourceCompression             string     `json:"sourceCompression"`
	ClientShowEncryptionParameter bool       `json:"clientShowEncryptionParameter"`
	EncryptionMaterial            string     `json:"encryptionMaterial"`
	StageInfo                     stageInfo  `json:"stageInfo"`
	Command                       string     `json:"command"`
	Operation                     string     `json:"operation"`
}

// Struct to hold response data for a normal query
type execResponse struct {
	Data    execResponseData `json:"Data"`
	Message string           `json:"message"`
	Code    string           `json:"code"`
	Success bool             `json:"success"`
}
