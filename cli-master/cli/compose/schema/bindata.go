// Code generated by "esc -o bindata.go -pkg schema -ignore .*\.go -private -modtime=1518458244 data"; DO NOT EDIT.

package schema

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"sync"
	"time"
)

type _escLocalFS struct{}

var _escLocal _escLocalFS

type _escStaticFS struct{}

var _escStatic _escStaticFS

type _escDirectory struct {
	fs   http.FileSystem
	name string
}

type _escFile struct {
	compressed string
	size       int64
	modtime    int64
	local      string
	isDir      bool

	once sync.Once
	data []byte
	name string
}

func (_escLocalFS) Open(name string) (http.File, error) {
	f, present := _escData[path.Clean(name)]
	if !present {
		return nil, os.ErrNotExist
	}
	return os.Open(f.local)
}

func (_escStaticFS) prepare(name string) (*_escFile, error) {
	f, present := _escData[path.Clean(name)]
	if !present {
		return nil, os.ErrNotExist
	}
	var err error
	f.once.Do(func() {
		f.name = path.Base(name)
		if f.size == 0 {
			return
		}
		var gr *gzip.Reader
		b64 := base64.NewDecoder(base64.StdEncoding, bytes.NewBufferString(f.compressed))
		gr, err = gzip.NewReader(b64)
		if err != nil {
			return
		}
		f.data, err = ioutil.ReadAll(gr)
	})
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (fs _escStaticFS) Open(name string) (http.File, error) {
	f, err := fs.prepare(name)
	if err != nil {
		return nil, err
	}
	return f.File()
}

func (dir _escDirectory) Open(name string) (http.File, error) {
	return dir.fs.Open(dir.name + name)
}

func (f *_escFile) File() (http.File, error) {
	type httpFile struct {
		*bytes.Reader
		*_escFile
	}
	return &httpFile{
		Reader:   bytes.NewReader(f.data),
		_escFile: f,
	}, nil
}

func (f *_escFile) Close() error {
	return nil
}

func (f *_escFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, nil
}

func (f *_escFile) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *_escFile) Name() string {
	return f.name
}

func (f *_escFile) Size() int64 {
	return f.size
}

func (f *_escFile) Mode() os.FileMode {
	return 0
}

func (f *_escFile) ModTime() time.Time {
	return time.Unix(f.modtime, 0)
}

func (f *_escFile) IsDir() bool {
	return f.isDir
}

func (f *_escFile) Sys() interface{} {
	return f
}

// _escFS returns a http.Filesystem for the embedded assets. If useLocal is true,
// the filesystem's contents are instead used.
func _escFS(useLocal bool) http.FileSystem {
	if useLocal {
		return _escLocal
	}
	return _escStatic
}

// _escDir returns a http.Filesystem for the embedded assets on a given prefix dir.
// If useLocal is true, the filesystem's contents are instead used.
func _escDir(useLocal bool, name string) http.FileSystem {
	if useLocal {
		return _escDirectory{fs: _escLocal, name: name}
	}
	return _escDirectory{fs: _escStatic, name: name}
}

// _escFSByte returns the named file from the embedded assets. If useLocal is
// true, the filesystem's contents are instead used.
func _escFSByte(useLocal bool, name string) ([]byte, error) {
	if useLocal {
		f, err := _escLocal.Open(name)
		if err != nil {
			return nil, err
		}
		b, err := ioutil.ReadAll(f)
		_ = f.Close()
		return b, err
	}
	f, err := _escStatic.prepare(name)
	if err != nil {
		return nil, err
	}
	return f.data, nil
}

// _escFSMustByte is the same as _escFSByte, but panics if name is not present.
func _escFSMustByte(useLocal bool, name string) []byte {
	b, err := _escFSByte(useLocal, name)
	if err != nil {
		panic(err)
	}
	return b
}

// _escFSString is the string version of _escFSByte.
func _escFSString(useLocal bool, name string) (string, error) {
	b, err := _escFSByte(useLocal, name)
	return string(b), err
}

// _escFSMustString is the string version of _escFSMustByte.
func _escFSMustString(useLocal bool, name string) string {
	return string(_escFSMustByte(useLocal, name))
}

var _escData = map[string]*_escFile{

	"/data/config_schema_v3.0.json": {
		local:   "data/config_schema_v3.0.json",
		size:    11063,
		modtime: 1518458244,
		compressed: `
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
/BUAAP//72YpJjcrAAA=
`,
	},

	"/data/config_schema_v3.1.json": {
		local:   "data/config_schema_v3.1.json",
		size:    12209,
		modtime: 1518458244,
		compressed: `
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
`,
	},

	"/data/config_schema_v3.2.json": {
		local:   "data/config_schema_v3.2.json",
		size:    13755,
		modtime: 1518458244,
		compressed: `
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
`,
	},

	"/data/config_schema_v3.3.json": {
		local:   "data/config_schema_v3.3.json",
		size:    15491,
		modtime: 1518458244,
		compressed: `
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
b/Iu7P/nxf8DAAD//7pHo+CDPAAA
`,
	},

	"/data/config_schema_v3.4.json": {
		local:   "data/config_schema_v3.4.json",
		size:    15874,
		modtime: 1518458244,
		compressed: `
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
`,
	},

	"/data/config_schema_v3.5.json": {
		local:   "data/config_schema_v3.5.json",
		size:    16802,
		modtime: 1518458244,
		compressed: `
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
`,
	},

	"/data/config_schema_v3.6.json": {
		local:   "data/config_schema_v3.6.json",
		size:    17084,
		modtime: 1518458244,
		compressed: `
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
oqbZ4ab+/7z5fwAAAP//nm8U9rxCAAA=
`,
	},

	"/data/config_schema_v3.7.json": {
		local:   "data/config_schema_v3.7.json",
		size:    17854,
		modtime: 1518458244,
		compressed: `
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
`,
	},

	"/data/config_schema_v3.8.json": {
		local:   "data/config_schema_v3.8.json",
		size:    18246,
		modtime: 1518458244,
		compressed: `
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
ean7MQBPP+U4w19V/z+t/hsAAP//Fd/bF0ZHAAA=
`,
	},

	"/": {
		isDir: true,
		local: "",
	},

	"/data": {
		isDir: true,
		local: "data",
	},
}
