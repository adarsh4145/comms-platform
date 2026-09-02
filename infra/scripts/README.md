# Container scripts

Bind-mounted read-only at `/scripts` by the services in `../docker-compose.yml` and invoked as
`bash /scripts/<name>.sh`.

They live here rather than inline in the compose file for two reasons. Compose interpolates `$`,
so every shell variable inside an inline script has to be written `$$VAR` — which is noise, and a
trap the moment you copy a line out to run it by hand. And an inline block scalar gets no syntax
highlighting, no shellcheck, and no line-continuations (see `.gitattributes`).

**These must stay LF.** A `\r` from a Windows checkout makes the container fail with
`$'\r': command not found`, so `.gitattributes` pins `*.sh` to LF. They are invoked through
`bash <path>` rather than executed directly, so a missing execute bit on a bind mount from Windows
does not matter.
