#!/usr/bin/env bash
set -euo pipefail

# --------------------------------------------------------------------
# 可配置参数
GOOS_ARG="${GOOS:-}"        # 允许在外部: GOOS=linux ./build_all.sh
GOARCH_ARG="${GOARCH:-}"    # 允许在外部: GOARCH=amd64 ./build_all.sh
PARALLEL="${PARALLEL:-1}"   # 并行度: 默认顺序。设为 $(nproc) 可加速
EXCLUDE_REGEX="${EXCLUDE_REGEX:-/adapter$}"  # 例: 排除 adapter 可执行。留空表示不过滤
LD_FLAGS='-s -w'            # 可追加版本信息 -X 变量
OUT_DIR="bin"
# --------------------------------------------------------------------

echo "[info] 输出目录: $OUT_DIR"
mkdir -p "$OUT_DIR"

echo "[info] 收集 main 包..."
PKGS=$(go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./cmd/... | grep -v '^$')

# 过滤（可选）
if [[ -n "$EXCLUDE_REGEX" ]]; then
  PKGS=$(echo "$PKGS" | grep -Ev "$EXCLUDE_REGEX" || true)
fi

echo "[info] 需要构建的包列表:"
echo "$PKGS" | sed 's/^/  - /'

build_one () {
  pkg="$1"
  name=$(basename "$pkg")
  out="$OUT_DIR/$name"
  echo "[build] $pkg -> $out"
  if [[ -n "$GOOS_ARG" && -n "$GOARCH_ARG" ]]; then
    GOOS="$GOOS_ARG" GOARCH="$GOARCH_ARG" go build -trimpath -ldflags "$LD_FLAGS" -o "$out" "$pkg"
  else
    go build -trimpath -ldflags "$LD_FLAGS" -o "$out" "$pkg"
  fi
}

export -f build_one
export OUT_DIR GOOS_ARG GOARCH_ARG LD_FLAGS

if [[ "$PARALLEL" -le 1 ]]; then
  echo "[info] 顺序构建..."
  while read -r p; do
    [[ -z "$p" ]] && continue
    build_one "$p"
  done <<< "$PKGS"
else
  echo "[info] 并行构建 (并发=$PARALLEL)..."
  # 需要 bash + xargs 支持
  echo "$PKGS" | xargs -I{} -P "$PARALLEL" bash -c 'build_one "$@"' _ {}
fi

echo "[info] 生成 SHA256 清单..."
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$OUT_DIR" && sha256sum * > manifest.sha256)
elif command -v shasum >/dev/null 2>&1; then
  (cd "$OUT_DIR" && shasum -a 256 * > manifest.sha256)
fi

echo "[done] 全部构建完成."

