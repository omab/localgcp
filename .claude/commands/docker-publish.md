# Publish localgcp Docker image to Docker Hub

Build a multi-platform Docker image and push it to `omab/localgcp` on Docker Hub.

## Steps

### 1. Determine the image tag

Read the current version from `pyproject.toml` (the `version = "..."` line under `[project]`).

The image will be tagged as both `omab/localgcp:<version>` and `omab/localgcp:latest`.

If `$ARGUMENTS` is provided, use that as the version tag instead (e.g. `/docker-publish 0.2.0`).

### 2. Pre-flight checks

- Run `git status` and confirm there are no uncommitted changes. If there are, stop and tell the user to commit or stash them first.
- Confirm Docker is running: `docker info > /dev/null 2>&1`. If it fails, tell the user Docker is not available.
- Confirm the user is logged in to Docker Hub: `docker info --format '{{.Username}}'`. If empty or an error, tell the user to run `! docker login` first and re-run this skill.

### 3. Set up multi-platform builder (if not already present)

Check for a buildx builder named `multiplatform`:
```
docker buildx inspect multiplatform 2>/dev/null
```

If it doesn't exist, create it:
```
docker buildx create --name multiplatform --use
docker buildx inspect --bootstrap
```

If it does exist, just select it:
```
docker buildx use multiplatform
```

### 4. Build and push the multi-platform image

```
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag omab/localgcp:<version> \
  --tag omab/localgcp:latest \
  --push \
  .
```

This builds for both AMD64 (standard servers/CI) and ARM64 (Apple Silicon, AWS Graviton) and pushes directly to Docker Hub.

If the build fails, show the full error output. Common causes:
- Missing `LICENSE` or `README.md` in the build context
- Network issues pulling base image

### 5. Verify the push

```
docker manifest inspect omab/localgcp:<version>
```

Confirm both `linux/amd64` and `linux/arm64` platforms are listed in the manifest.

### 6. Report

Print a summary:
- Image: `omab/localgcp:<version>` and `omab/localgcp:latest`
- Platforms: `linux/amd64`, `linux/arm64`
- Docker Hub URL: `https://hub.docker.com/r/omab/localgcp`
