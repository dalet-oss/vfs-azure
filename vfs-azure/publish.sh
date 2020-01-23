echo "Determining version number for publication"

echo "Looking for an existing release tag against this commit"
VERSION=$(git describe --match release/* --exact-match 2>&1)
if [ $? -ne 0 ]
then
  LAST=$(git describe --match release/* 2>&1)
  if [ $? -ne 0 ]
  then
    echo "No release tags found at all; bail out"
    exit 1
  fi

  echo "No matching tag found. Push a tag like release/1.0 against HEAD in order to release.  Most recent tag is: ${LAST}"
  exit 0
fi

VERSION=$(echo $VERSION | sed 's#release/##g')
echo "Publishing version: ${VERSION}"

status=$(curl -s --head -w %{http_code} -o /dev/null https://dl.bintray.com/dalet-oss/maven/com/dalet/sludev/commons/vfs-azure/${VERSION}/)
if [ $status -eq 200 ]
then
  echo "Version already published - nothing to do here"
else
  mvn deploy -DperformRelease=true -Drevision=${VERSION}
fi
