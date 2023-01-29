# syntax=docker/dockerfile:1

FROM clux/muslrust:stable as builder

# download the index
RUN cargo search lazy_static
ADD Cargo.* .
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo fetch
ENV CARGO_PROFILE_RELEASE_LTO=true
RUN cargo check --release --target x86_64-unknown-linux-musl
ADD . .
RUN cargo build --release

FROM alpine:3
USER 65534

#RUN mkdir /data && chown 65534:65534 /data
WORKDIR /data
COPY --from=builder /volume/target/x86_64-unknown-linux-musl/release/batchy /opt
EXPOSE 3000
CMD ["/opt/batchy"]
