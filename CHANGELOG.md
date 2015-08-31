# Changelog

## dev

  * Breaking changes:
    * Raise on `limit` and `offset` in `update_all` and `delete_all` queries,
      it's not supported by MongoDB, we were failing siletnly before

  * Bug fixes:
    * Allow interpolation in limit and offset

## v0.1.1

  * Bug fixes:
    * Fix logging issues on find queries

## v0.1.0

First release
