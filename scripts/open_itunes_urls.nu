#!/usr/bin/env nu

def main [] {
  # print cwd:
  print $env.PWD
  let res = (open missing_tracks.yaml | from yaml)

  return res
}