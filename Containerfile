FROM rust:1.70 AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./

RUN --mount=type=cache,target=/root/.cargo \
    cargo fetch

COPY . .

RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install fish
RUN groupadd -r thor && useradd --shell /usr/bin/fish --create-home -g thor thor
USER thor

WORKDIR /app

COPY --from=builder /build/target/release/libmidwest_mainline.rlib .

CMD ["/usr/bin/fish"]
