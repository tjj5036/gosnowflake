package gosnowflake

import (
	"sync"
	)

type fileType struct {
	name           string
	file_extension string
	mime_type      string
	mime_subtypes  []string
	is_supported   bool
}

var supportedFileTypesSync sync.Once
var supportedMimeTypes = map[string]fileType{}

var allFileTypes = []fileType{
	{
		name:           "GZIP",
		file_extension: ".gz",
		mime_type:      "application",
		mime_subtypes:  []string{"gzip", "x-gzip"},
		is_supported:   true,
	},
	{
		name:           "DEFLATE",
		file_extension: ".deflate",
		mime_type:      "application",
		mime_subtypes:  []string{"zlib", "deflate"},
		is_supported:   true,
	},

	{
		name:           "RAW_DEFLATE",
		file_extension: ".raw_deflate",
		mime_type:      "application",
		mime_subtypes:  []string{"raw_deflate"},
		is_supported:   true,
	},
	{
		name:           "BZIP2",
		file_extension: ".bz2",
		mime_type:      "application",
		mime_subtypes:  []string{"bzip2", "x-bzip2", "x-bz2", "x-bzip", "bz2"},
		is_supported:   true,
	},

	{
		name:           "LZIP",
		file_extension: ".lz",
		mime_type:      "application",
		mime_subtypes:  []string{"lzip", "x-lzip"},
		is_supported:   false,
	},

	{
		name:           "LZMA",
		file_extension: ".lzma",
		mime_type:      "application",
		mime_subtypes:  []string{"lzma", "x-lzma"},
		is_supported:   false,
	},
	{
		name:           "LZO",
		file_extension: ".lzo",
		mime_type:      "application",
		mime_subtypes:  []string{"lzo", "x-lzo"},
		is_supported:   false,
	},
	{
		name:           "XZ",
		file_extension: ".xz",
		mime_type:      "application",
		mime_subtypes:  []string{"xz", "x-xz"},
		is_supported:   false,
	},
	{
		name:           "COMPRESS",
		file_extension: ".Z",
		mime_type:      "application",
		mime_subtypes:  []string{"compress", "x-compress"},
		is_supported:   false,
	},
	{
		name:           "PARQUET",
		file_extension: ".parquet",
		mime_type:      "snowflake",
		mime_subtypes:  []string{"parquet"},
		is_supported:   true,
	},
	{
		name:           "ZSTD",
		file_extension: ".zst",
		mime_type:      "application",
		mime_subtypes:  []string{"zstd", "x-zstd"},
		is_supported:   true,
	},
	{
		name:           "BROTLI",
		file_extension: ".br",
		mime_type:      "application",
		mime_subtypes:  []string{"br", "x-br"},
		is_supported:   true,
	},
	{
		name:           "ORC",
		file_extension: ".orc",
		mime_type:      "snowflake",
		mime_subtypes:  []string{"orc"},
		is_supported:   true,
	},
}

// Builds the map of mime_subtypes to file formats
func buildMap() {
	for i:= 0; i < len(allFileTypes); i++ {
		for j := 0; j < len(allFileTypes[i].mime_subtypes); j++ {
			ms := allFileTypes[i].mime_subtypes[j]
			supportedMimeTypes[ms] = allFileTypes[i]
		}
	}
}

// Gets the file type associated with a mime type
func GetFileTypeForMimeType(mimeType string) (fileType, bool) {
	if v, ok := supportedMimeTypes[mimeType]; ok {
		return v, true
	}
	return fileType{}, false
}

// Initializer to build the map of mime_subtypes to file formats only once
func InitFileTypes() {
	supportedFileTypesSync.Do(buildMap)
}
