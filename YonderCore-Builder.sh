#!/bin/bash

set -e

cargo clean
rm -f Cargo.lock

sleep 15

mkdir -p target/headers
cp YonderCore.h target/headers/

cat <<EOF > target/headers/module.modulemap
module YonderCore {
    header "YonderCore.h"
    export *
}
EOF

cargo build --release --target aarch64-apple-ios
cargo build --release --target aarch64-apple-ios-macabi
cargo build --release --target aarch64-apple-ios-sim
cargo build --release --target x86_64-apple-ios
cargo build --release --target aarch64-apple-darwin
cargo build --release --target x86_64-apple-darwin

mkdir -p target/ios-simulator
mkdir -p target/macos-universal

lipo -create \
  target/aarch64-apple-ios-sim/release/libyonder_core.a \
  target/x86_64-apple-ios/release/libyonder_core.a \
  -output target/ios-simulator/libyonder_core.a

lipo -create \
  target/aarch64-apple-darwin/release/libyonder_core.a \
  target/x86_64-apple-darwin/release/libyonder_core.a \
  -output target/macos-universal/libyonder_core.a

xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/libyonder_core.a \
  -headers target/headers \
  -library target/aarch64-apple-ios-macabi/release/libyonder_core.a \
  -headers target/headers \
  -library target/ios-simulator/libyonder_core.a \
  -headers target/headers \
  -library target/macos-universal/libyonder_core.a \
  -headers target/headers \
  -output target/YonderCore.xcframework
