FROM alpine:3.22.1
RUN /sbin/apk --no-cache add ca-certificates bash

ARG TARGETOS
ARG TARGETARCH

ADD "bin/${TARGETOS}_${TARGETARCH}/provider" /usr/local/bin/provider

ENV USER_ID=65532

# The provider now runs on the upjet no-fork (Terraform Plugin SDKv2,
# in-process) runtime. The Terraform CLI, the native provider binary mirror and
# the TERRAFORM_* runtime environment used by the old CLI-based runtime are no
# longer required and have been removed: the Terraform provider is compiled into
# the provider binary via its Go module dependency.

USER ${USER_ID}
EXPOSE 8080

ENTRYPOINT ["provider"]
