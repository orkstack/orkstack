# images/Makefile — the first-party activity images.
#
# The first-party images, built exactly like any third-party image would be.
# There is no privileged internal path: `docker build` is the whole toolchain,
# and the result is an ordinary OCI image an Orker pulls by digest.
#
# §4.6 of docs/OCI_ACTIVITY_CONTRACT.md is the delivery model. Its key point is
# that there is no *unpushed* local image, because a locally built, never-pushed
# image has no registry manifest digest — `--iidfile` gives a local config ID,
# which is not a manifest digest and does not transfer to a second Orker. So:
#
#   make images    build and tag locally. Enough to run the contract fixture.
#   make push      push to REGISTRY, which is what produces a real index digest.
#   make layout    export the OCI image layouts the embedded fallback reads.
#   make release   every step, in that order — the full §4.6 delivery.
#   make oci       build every image ONCE as an OCI image layout under OUT, with
#                  no registry and no daemon tag: what a CI proof's build part
#                  ships, and `proofrun push-layout` copies byte for byte.
#   make contract  run the contract fixtures on REGISTRY/…:TAG as they are, with
#                  no build (ACTIVITY= runs one); `make test` builds, then runs it.
#
# The local registry is `registry:3` from deploy/docker-compose.yml on port
# 5001; `push`, `layout` and `release` need it running.

SHELL := /bin/bash

REGISTRY ?= localhost:5001
NAMESPACE ?= orkstack
TAG ?= dev

# LAYOUT_DIR mirrors ORK_ORKER_IMAGE_LAYOUT's default (§4.6).
LAYOUT_DIR ?= $(CURDIR)/.layout

ACTIVITIES := shell http file json git template

# DOCKER_BUILD_FLAGS is passed to every image build, e.g. an air-gapped
# builder's --build-arg GOMPLATE_BASE_URL=<mirror> for activity-template (whose
# Dockerfile verifies the downloaded binary against its pinned SHA-256 whatever
# served it). Images that declare no such ARG ignore it; BuildKit only warns.
DOCKER_BUILD_FLAGS ?=

# ref/1 is the one place a first-party image reference is spelled.
ref = $(REGISTRY)/$(NAMESPACE)/activity-$(1):$(TAG)

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

.PHONY: images
images: $(addprefix image-,$(ACTIVITIES)) ## Build and tag every first-party activity image

.PHONY: $(addprefix image-,$(ACTIVITIES))
$(addprefix image-,$(ACTIVITIES)): image-%: ## Build one activity image
	docker build $(DOCKER_BUILD_FLAGS) --tag '$(call ref,$*)' activity-$*
	@echo "built $(call ref,$*)"

.PHONY: push
push: images ## Push every image to REGISTRY (this is what mints the index digest)
	@for a in $(ACTIVITIES); do \
	  docker push "$(REGISTRY)/$(NAMESPACE)/activity-$$a:$(TAG)" || exit 1; \
	done
	@echo
	@echo "pinned references — paste into ORK_ACTIVITY_CATALOG:"
	@for a in $(ACTIVITIES); do \
	  d=$$(docker buildx imagetools inspect "$(REGISTRY)/$(NAMESPACE)/activity-$$a:$(TAG)" \
	        --format '{{.Manifest.Digest}}' 2>/dev/null) || exit 1; \
	  printf '  %-8s %s/%s/activity-%s@%s\n' "$$a" "$(REGISTRY)" "$(NAMESPACE)" "$$a" "$$d"; \
	done

.PHONY: layout
layout: push ## Export each pushed image as an OCI image layout for the embedded fallback
	@mkdir -p '$(LAYOUT_DIR)'
	@for a in $(ACTIVITIES); do \
	  echo "exporting activity-$$a -> $(LAYOUT_DIR)/activity-$$a"; \
	  rm -rf "$(LAYOUT_DIR)/activity-$$a"; \
	  docker buildx build $(DOCKER_BUILD_FLAGS) --output \
	    "type=oci,dest=$(LAYOUT_DIR)/activity-$$a.tar,tar=true" \
	    --tag "$(REGISTRY)/$(NAMESPACE)/activity-$$a:$(TAG)" \
	    "activity-$$a" || exit 1; \
	  mkdir -p "$(LAYOUT_DIR)/activity-$$a"; \
	  tar -xf "$(LAYOUT_DIR)/activity-$$a.tar" -C "$(LAYOUT_DIR)/activity-$$a" || exit 1; \
	  rm -f "$(LAYOUT_DIR)/activity-$$a.tar"; \
	done

.PHONY: release
release: layout ## The full §4.6 delivery: build, push, and export layouts

.PHONY: print-activities
print-activities: ## Print the first-party activity names, for scripts
	@echo $(ACTIVITIES)

.PHONY: oci
oci: ## Build every image once as an OCI image layout into OUT/activity-<name>/: no push, no daemon tag
	$(if $(strip $(OUT)),,$(error make oci needs OUT=<directory> for the layouts (absolute, or relative to images/)))
	@mkdir -p '$(abspath $(OUT))'
	@for a in $(ACTIVITIES); do \
	  echo "building activity-$$a -> $(abspath $(OUT))/activity-$$a"; \
	  rm -rf "$(abspath $(OUT))/activity-$$a"; \
	  docker buildx build $(DOCKER_BUILD_FLAGS) --provenance=false --sbom=false \
	    --output "type=oci,dest=$(abspath $(OUT))/activity-$$a,tar=false" \
	    "activity-$$a" || exit 1; \
	done

# The activities the contract fixtures cover: shell is the bash fixture's
# reference image; json, git and template have a first-party fixture each.
CONTRACT_ACTIVITIES := shell json git template

.PHONY: contract
contract: ## Run the contract fixtures on REGISTRY/NAMESPACE/activity-*:TAG with no build; ACTIVITY= runs one
	$(if $(filter-out $(CONTRACT_ACTIVITIES),$(ACTIVITY)),$(error ACTIVITY must be one of $(CONTRACT_ACTIVITIES), not $(ACTIVITY)))
	@for a in $(or $(ACTIVITY),$(CONTRACT_ACTIVITIES)); do \
	  ref="$(REGISTRY)/$(NAMESPACE)/activity-$$a:$(TAG)"; \
	  docker image inspect "$$ref" >/dev/null 2>&1 || \
	    { echo "$$ref is not in the docker daemon: build it (make test) or docker pull it; make contract never builds"; exit 1; }; \
	  if [ "$$a" = shell ]; then \
	    bash ../test/fixtures/bash_activity_contract.sh "$$ref" || exit $$?; \
	  else \
	    bash ../test/fixtures/first_party_contract.sh "$$a" "$$ref" || exit $$?; \
	  fi; \
	done

.PHONY: test
test: image-shell image-json image-git image-template ## Build the fixtures' images, then run the contract fixtures on them
	$(MAKE) contract

.PHONY: clean
clean: ## Remove locally built image tags and exported layouts
	-@for a in $(ACTIVITIES); do \
	  docker image rm -f "$(REGISTRY)/$(NAMESPACE)/activity-$$a:$(TAG)" 2>/dev/null; \
	done
	rm -rf '$(LAYOUT_DIR)'
