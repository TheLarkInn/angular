set -e

export NVM_DIR="/Users/Shared/jenkins/nvm"
. "$NVM_DIR/nvm.sh"  # This loads nvm
export ANDROID_SDK="/Users/Shared/jenkins/android-sdk"
export PATH+=":$ANDROID_SDK/tools:$ANDROID_SDK/platform-tools"
export PATH+=":/usr/local/git/bin"

export CHANNEL=stable
export ARCH=macos-ia32
export PERF_BROWSERS=ChromeAndroid
export CLOUD_SECRET_PATH="/Users/Shared/jenkins/keys/perf-cloud-secret"

nvm use 0.10

./scripts/travis/install_dart.sh
npm install
./scripts/travis/build.sh
./scripts/travis/test_perf.sh