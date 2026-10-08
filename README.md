# ya-to-dbx-sync

Copies files from Yandex Disk to Dropbox, skipping files that are already newer on Dropbox. The copy is done
server-side. On first run it asks for OAuth tokens and caches them locally.

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

Upload them into the `backup` sub-folder of the Dropbox folder:

```sh
./ya-to-dbx-sync -action to-dropbox -dirs A,B -dst-path backup
```

## License

MIT
