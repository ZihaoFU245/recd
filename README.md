**Build and Run**

```bash
go build -o recd main.go

./recd --extra-headers=<Json file> config.json
```

**Config**

Config uses standard json, remove all comments to use.

```json5
[
  {
    "is_paused": false,
    "username": "<fill it>",
    "framerate": 30,
    "resolution": 720,
    /* Output path and file names*/
    "pattern": "out/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}",
    "max_duration": 120,
    "max_filesize": 0,
    "created_at": 0
  }
]
```

headers example

```json5
{
  "User-Agent": "",
  /* Other headers, like cf_clearance */
}
```
