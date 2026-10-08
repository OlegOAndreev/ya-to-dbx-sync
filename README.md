# ya-to-dbx-sync

Uploads files from Yandex Disk to Dropbox, skipping files that are already newer on Dropbox. On first run it asks for
OAuth tokens and caches them locally.

## Build

```sh
go build
```

## Usage

List every file on the disk:

```sh
./ya-to-dbx-sync -action list-all
```

List top-level directories:

```sh
./ya-to-dbx-sync -action list-top
```

Upload all files:

```sh
./ya-to-dbx-sync -action to-dropbox
```

Upload only the files from the directories A and B:

```sh
./ya-to-dbx-sync -action to-dropbox -dirs A,B
```

## License

MIT
