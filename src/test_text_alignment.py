#!/usr/bin/env python3
"""
Test script to generate sample playlist covers and verify text alignment.
"""

from image_generator import generate_playlist_cover


def test_text_alignment():
  """Generate test images to verify text alignment."""

  # Test cases with different text lengths
  test_cases = [
    {
      'name': 'short_text',
      'caption': 'Liquid Weekly\n2025 - #30',
      'image': 'purple_fluid.jpg'
    },
    {
      'name': 'long_text',
      'caption': 'Underground Weekly\n2025 - #30\nSpecial Edition',
      'image': 'bw_lines.jpg'
    },
    {
      'name': 'very_long_text',
      'caption': 'This is a very long playlist name that should wrap to multiple lines and test the bottom alignment properly',
      'image': 'purple_fluid.jpg'
    },
    {
      'name': 'single_line',
      'caption': 'Single Line Test',
      'image': 'bw_lines.jpg'
    }
  ]

  print("🎨 Testing text alignment with sample images...")

  for test_case in test_cases:
    print(f"\n📝 Testing: {test_case['name']}")
    print(f"📝 Caption: {test_case['caption']}")

    output_filename = f"test_alignment_{test_case['name']}.png"

    result = generate_playlist_cover(
      image_path=test_case['image'],
      caption=test_case['caption'],
      output_path=output_filename
    )

    if result:
      print(f"✅ Generated: {output_filename}")
      print(f"📏 Size: {result.size}")
    else:
      print(f"❌ Failed to generate: {output_filename}")

  print(f"\n🎯 Test images generated! Check the files to verify text alignment.")


if __name__ == "__main__":
  test_text_alignment()
