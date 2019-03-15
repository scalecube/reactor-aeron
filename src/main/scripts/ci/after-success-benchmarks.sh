#!/usr/bin/env bash

remoteBenchmarks() {
    echo ++++ Remote benchmarks ++++
    tokenreq='{"username":"'$SCALECUBE_CD_USERNAME'","password":"'$SCALECUBE_CD_PASSWORD'"}'
    token=$(curl -X POST -H "Content-Type: application/json" -d "$tokenreq" ${CD_SERVER}auth | jq -r '.access_token')
    authorization="Authorization: Bearer $token"
    buildreq='{"branch":"'$TRAVIS_PULL_REQUEST_BRANCH'",
               "pull_request":"'$TRAVIS_PULL_REQUEST'",
               "repo_slug":"'$TRAVIS_REPO_SLUG'",
               "github_user":"'$GITHUBUSER'",
               "github_token":"'$GITHUBTOKEN'",
               "sha":"'$TRAVIS_PULL_REQUEST_SHA'"}'
    curl -d """$buildreq""" -H "Content-Type: application/json" -H "$authorization" -X POST ${CD_SERVER}benchmark/aeron_test
}

if [ ! "$TRAVIS_PULL_REQUEST" == "false" ]; then
    BENCHMARKS=$(curl -s -u "$GITHUBUSER:$GITHUBTOKEN" https://api.github.com/repos/$TRAVIS_REPO_SLUG/pulls/$TRAVIS_PULL_REQUEST \
                 | jq ".labels[].name | select (.==\"$BENCHMARKS_LABEL\")")
    if [ ! -z "$BENCHMARKS" ]; then
        remoteBenchmarks
    fi 
fi
