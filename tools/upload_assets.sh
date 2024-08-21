#!/usr/bin/env bash

VERSION=$1

# repo
REPO ="cfanbo/minkv"
# softname
NAME="minkv"

# bucket name
BUCKET="oss://githubfiles/minkv"

LIST=(
    "$NAME"-v"$VERSION"-x86_64-unknown-linux-gnu.tar.gz
    "$NAME"-v"$VERSION"-aarch64-unknown-linux-gnu.tar.gz
    "$NAME"-v"$VERSION"-x86_64-apple-darwin.tar.gz
    "$NAME"-v"$VERSION"-aarch64-apple-darwin.tar.gz
    "$NAME"-v"$VERSION"-x86_64-pc-windows-msvc.zip
    "$NAME"-v"$VERSION"-aarch64-pc-windows-msvc.zip
)

for filename in "${LIST[@]}"
do
    curl -fsSL -O \
        -H "Authorization: Bearer $GITHUB_TOKEN" \
        https://github.com/"$REPO"/releases/download/v"$VERSION"/"$filename"
    shasum -a 256 "$filename" >> SHASUMS256.txt
done

cat ./SHASUMS256.txt


# install aliyun script
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/aliyun/aliyun-cli/HEAD/install.sh)"

# upload assets
FLAGS="${BUCKET}/ --force --access-key-id ${ACCESS_KEY_ID} --access-key-secret ${ACCESS_KEY_SECRET} --region cn-shanghai"
echo $LFLAGS
for filename in "${LIST[@]}"
do
    aliyun oss cp $filename $FLAGS
done

# create-symlink
# aliyun oss create-symlink oss://githubfiles/test/v1.2.3.tar.gz oss://githubfiles/test/latest_symlink_name.txt --region cn-shanghai
for filename in "${LIST[@]}"
do
    origin_name="${BUCKET}/${filename}"

    file="${filename/v$VERSION/latest}"
    latest="${BUCKET}/${file}"
    aliyun oss create-symlink $latest $origin_name --region cn-shanghai
done
