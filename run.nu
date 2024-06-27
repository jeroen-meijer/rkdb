#!/usr/bin/env nu

def main [
  command: string
] {
  overlay use venv/bin/activate.nu
  python3 src/main.py $command
}