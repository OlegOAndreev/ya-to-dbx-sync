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

Upload only the files from the directories A and B to sub-folder:

```sh
./ya-to-dbx-sync -action to-dropbox -dirs A,B -to-path backup
```

Authorize Google Drive and store the OAuth token locally (in `gdrive.token` by default), doing nothing else:

```sh
./ya-to-dbx-sync -action gdrive-auth -gdrive-client-id CLIENT_ID -gdrive-client-secret CLIENT_SECRET
```

## Secrets

The tokens for sync are stored locally in `*.token` files. Do not forget to remove them when they are no longer used.

## Privacy policy

* The tool runs on the user's own computer.
* OAuth tokens are stored in a file on that computer, readable only by the user's account. They are never copied elsewhere.
* User data is not stored, shared, sold, or sent to any third party.
* The tool sends messages only when the user asks it to.
* Remove the credentials and revoke access at any time at:
  * https://myaccount.google.com/permissions
  * https://id.yandex.ru/personal/data-access
  * https://www.dropbox.com/account/connected_apps

## Terms of service

The tool is provided as is, for the author's own use, with no warranty.

## License

MIT
