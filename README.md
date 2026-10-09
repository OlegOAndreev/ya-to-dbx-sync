# ya-to-dbx-sync

Uploads files from Yandex Disk to Dropbox or Google Drive, skipping files that are already newer there. On first run
it asks for OAuth tokens and caches them locally.

## Build

```sh
go build
```

## Google Drive Credentials

The tool works out of the box with Yandex.Disk and Dropbox when built from source. Unfortunately, Google Drive requires
client credentials to be secret.

Our release builds contain a client credentials builtin, but if you need to build the app from source, you need to pass
your own client credentials.

The easiest way to do this is to add a new client for yourselves by following instructions in
https://developers.google.com/workspace/drive/api/quickstart/go. After you create your credentials, store them in the
file and pass -gdrive-client-file argument (note that this you will still need to create the OAuth token, but it will be
created for your newly created client).

Do not forget to add yourselves to the list of test users!

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

Upload only the files from the directories A and B to sub-folder:

```sh
./ya-to-dbx-sync -action to-dropbox -dirs A,B -to-path backup
```

Upload all files to Google Drive (same flags as `to-dropbox`):

```sh
./ya-to-dbx-sync -action to-drive
```

Authorize Google Drive and store the OAuth token locally (in `gdrive.token` by default), doing nothing else:

```sh
./ya-to-dbx-sync -action gdrive-auth
```

## Secrets

The tokens for sync are stored locally in `*.token` files. Do not forget to remove them when they are no longer used.

## License

MIT
