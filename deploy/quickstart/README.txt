OrkStack quickstart

This directory runs the whole OrkStack stack from one release's images, with
Docker as the only prerequisite. Its .env names every image by version and
digest.

  docker compose up -d --wait                        start it
  alias orkctl='docker compose run --rm orkctl'      orkctl, from its image
  orkctl apply examples/each-fan-out.ork.yaml        store an example
  orkctl run submit each-fan-out --watch             run it
  docker compose down                                stop it

The UI is at http://localhost:5173 and the engine's API at
http://localhost:8080. Change a port in .env (ORKSTACK_UI_PORT,
ORKSTACK_API_PORT, ORKSTACK_TEMPORAL_UI_PORT) if it is taken.

The Orker runs each task as a container on this machine's Docker, through the
Docker socket the compose file gives it: whoever controls that socket
controls the machine. Listening on this machine only is not a login: any
program on it, or a web page by DNS rebinding, can start a run. Stop the
stack when you are done. The object store's keys in compose.yaml are
placeholders; only the stack's own services reach the store.

Getting started: https://orkstack.github.io/orkstack/docs/tutorials/first-orkflow/
Source and full documentation: https://github.com/orkstack/orkstack
