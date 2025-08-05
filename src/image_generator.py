import os
import re
from typing import Optional, Dict, Any, List
from PIL import Image, ImageDraw, ImageFont
import requests
from io import BytesIO
import urllib.parse


def is_url(image_path: str) -> bool:
  """Check if the image path is a URL."""
  return image_path.startswith(('http://', 'https://'))


def load_image(image_path: str, assets_dir: str = "assets/images") -> Optional[Image.Image]:
  """
  Load an image from either a local file or URL.

  Args:
      image_path: Path to image file or URL
      assets_dir: Directory containing local image assets

  Returns:
      PIL Image object or None if loading fails
  """
  try:
    if is_url(image_path):
      # Load from URL
      response = requests.get(image_path, timeout=10)
      response.raise_for_status()
      return Image.open(BytesIO(response.content))
    else:
      # Load from local file
      local_path = os.path.join(assets_dir, image_path)
      if not os.path.exists(local_path):
        return None
      return Image.open(local_path)
  except Exception as e:
    print(f"⚠️  Warning: Could not load image '{image_path}': {e}")
    return None


def crop_to_square(image: Image.Image) -> Image.Image:
  """
  Crop an image to a square, centering the content.

  Args:
      image: PIL Image object

  Returns:
      Cropped square image
  """
  width, height = image.size

  # Find the smaller dimension
  size = min(width, height)

  # Calculate crop box to center the image
  left = (width - size) // 2
  top = (height - size) // 2
  right = left + size
  bottom = top + size

  return image.crop((left, top, right, bottom))


def wrap_text(text: str, font: ImageFont.FreeTypeFont, max_width: int) -> List[str]:
  """
  Wrap text to fit within a maximum width.

  Args:
      text: Text to wrap
      font: Font to use for measuring
      max_width: Maximum width in pixels

  Returns:
      List of wrapped lines
  """
  words = text.split()
  lines = []
  current_line = []

  for word in words:
      # Test adding the word to current line
    test_line = ' '.join(current_line + [word])
    bbox = ImageDraw.Draw(Image.new('RGB', (1, 1))).textbbox(
      (0, 0), test_line, font=font)
    line_width = bbox[2] - bbox[0]

    if line_width <= max_width:
      # Word fits, add it to current line
      current_line.append(word)
    else:
      # Word doesn't fit
      if current_line:
        # Save current line and start new one
        lines.append(' '.join(current_line))
        current_line = [word]
      else:
        # Single word is too long, break it
        # Try to break at reasonable points
        if len(word) > 10:  # Only break very long words
          # Try to break at common break points
          break_points = ['-', '_', '.']
          broken = False
          for bp in break_points:
            if bp in word:
              parts = word.split(bp, 1)
              if parts[0]:
                lines.append(parts[0] + bp)
                current_line = [parts[1]] if parts[1] else []
                broken = True
                break
          if not broken:
            # Force break at middle
            mid = len(word) // 2
            lines.append(word[:mid] + '-')
            current_line = [word[mid:]]
        else:
          # Just add the word even if it's a bit long
          current_line = [word]

  # Add remaining line
  if current_line:
    lines.append(' '.join(current_line))

  return lines


def create_gradient_overlay(size: int) -> Image.Image:
  """
  Create a gradient overlay that darkens the bottom half for better text visibility.

  Args:
      size: Size of the image (square)

  Returns:
      Gradient overlay image
  """
  # Create a gradient from transparent at top to dark at bottom
  gradient = Image.new('RGBA', (size, size), (0, 0, 0, 0))
  draw = ImageDraw.Draw(gradient)

  # Start gradient at middle (50% of image height)
  start_y = size // 2

  for y in range(start_y, size):
      # Calculate alpha (transparency) - goes from 0 to 180 (not fully opaque)
      # This creates a dark but not black overlay
    alpha = int(((y - start_y) / (size - start_y)) * 180)
    color = (0, 0, 0, alpha)

    # Draw a horizontal line at this y position
    draw.line([(0, y), (size, y)], fill=color)

  return gradient


def generate_playlist_cover(
    image_path: str,
    caption: str,
    font_path: str = "assets/fonts/Manrope-ExtraBold.ttf",
    output_path: Optional[str] = None,
    size: int = 512
) -> Optional[Image.Image]:
  """
  Generate a playlist cover image with text overlay.

  Args:
      image_path: Path to background image (local file or URL)
      caption: Text to overlay on the image
      font_path: Path to the font file
      output_path: Optional path to save the generated image
      size: Size of the output image (square)

  Returns:
      Generated PIL Image object or None if generation fails
  """
  try:
    # Load background image
    background = load_image(image_path)
    if background is None:
      print(f"⚠️  Warning: Could not load image '{image_path}'")
      return None

    # Crop to square
    background = crop_to_square(background)

    # Resize to target size
    background = background.resize((size, size), Image.Resampling.LANCZOS)

    # Create a new image with the background
    result = Image.new('RGB', (size, size))
    result.paste(background, (0, 0))

    # Add gradient overlay for better text visibility
    gradient = create_gradient_overlay(size)
    result = Image.alpha_composite(
      result.convert('RGBA'), gradient).convert('RGB')

    # Load font
    try:
      # Start with a large font size and scale down
      font_size = size // 8
      font = ImageFont.truetype(font_path, font_size)
    except Exception as e:
      print(f"⚠️  Warning: Could not load font '{font_path}': {e}")
      # Fallback to default font
      font = ImageFont.load_default()

    # Create drawing object
    draw = ImageDraw.Draw(result)

    # Calculate text positioning
    # Calculate available width for text (with padding)
    padding = size // 12  # Increased from 5% to ~8.3% of image size as padding
    max_text_width = size - (2 * padding)

    # Split caption into lines and wrap long lines
    original_lines = caption.split('\n')
    lines = []
    for line in original_lines:
      wrapped_lines = wrap_text(line, font, max_text_width)
      lines.extend(wrapped_lines)

    # Calculate proper line height using actual text bounding box
    if lines:
      # Get the bounding box of a sample line to calculate proper line height
      sample_bbox = draw.textbbox((0, 0), lines[0], font=font)
      line_height = sample_bbox[3] - sample_bbox[1] + 4  # Add 4px spacing between lines
    else:
      line_height = font_size + 4

    # Calculate total text height
    total_text_height = len(lines) * line_height

    # Position text anchored to the bottom with proper padding
    # Text grows upward from the bottom if there are more lines
    bottom_padding = padding
    start_y = size - bottom_padding - total_text_height

    # Draw each line
    for i, line in enumerate(lines):
      # Get text bounding box
      bbox = draw.textbbox((0, 0), line, font=font)
      text_width = bbox[2] - bbox[0]

      # Left-align text with padding
      x = padding

      # Calculate y position for this line
      y = start_y + i * line_height

      # Draw text with white color and black outline for better visibility
      # Draw outline first
      for dx in [-1, 0, 1]:
        for dy in [-1, 0, 1]:
          if dx != 0 or dy != 0:
            draw.text((x + dx, y + dy), line, fill='black', font=font)

      # Draw main text
      draw.text((x, y), line, fill='white', font=font)

    # Save if output path is provided
    if output_path:
      # Spotify requires JPEG format for playlist covers
      if output_path.lower().endswith('.png'):
        # Convert to JPEG path
        jpeg_path = output_path.replace('.png', '.jpg')
        result.save(jpeg_path, 'JPEG', quality=95)
        print(f"✅ Generated playlist cover: {jpeg_path}")
        return result
      else:
        result.save(output_path, 'JPEG', quality=95)
        print(f"✅ Generated playlist cover: {output_path}")

    return result

  except Exception as e:
    print(f"❌ Error generating playlist cover: {e}")
    return None


def upload_playlist_image(sp, playlist_id: str, image_path: str) -> bool:
  """
  Upload a playlist cover image to Spotify.

  Args:
      sp: Spotify client
      playlist_id: Spotify playlist ID
      image_path: Path to the image file

  Returns:
      True if upload successful, False otherwise
  """
  try:
    # Read image file
    with open(image_path, 'rb') as f:
      image_data = f.read()

    # Spotify API requires base64 encoded image data
    import base64
    base64_image = base64.b64encode(image_data).decode('utf-8')

    # Upload to Spotify
    sp.playlist_upload_cover_image(playlist_id, base64_image)
    print(f"✅ Uploaded playlist cover to Spotify")
    return True

  except Exception as e:
    print(f"❌ Error uploading playlist cover: {e}")
    return False


def process_playlist_cover(
    sp,
    job: Dict[str, Any],
    playlist_id: str,
    playlist_name: str,
    output_dir: str = "build/generated_covers"
) -> bool:
  """
  Process playlist cover generation and upload for a job.

  Args:
      sp: Spotify client
      job: Job configuration
      playlist_id: Spotify playlist ID
      playlist_name: Name of the playlist
      output_dir: Directory to save generated images

  Returns:
      True if successful, False otherwise
  """
  cover_config = job.get('cover')
  if not cover_config:
    return True  # No cover requested

  image_path = cover_config.get('image')
  if not image_path:
    print("⚠️  Warning: Cover config missing 'image' field")
    return False

  # Get caption from config or use playlist name
  caption = cover_config.get('caption', playlist_name)

  # Apply template variables to caption
  caption = apply_template_variables(caption, job, playlist_name)

  # Create output directory if it doesn't exist
  os.makedirs(output_dir, exist_ok=True)

  # Generate cover image
  output_filename = f"{job.get('name', 'playlist')}_cover.png"
  output_path = os.path.join(output_dir, output_filename)

  generated_image = generate_playlist_cover(
      image_path=image_path,
      caption=caption,
      output_path=output_path
  )

  if generated_image is None:
    return False

  # The generate_playlist_cover function converts .png to .jpg for Spotify
  # So we need to use the JPEG path for upload
  jpeg_path = output_path.replace('.png', '.jpg')

  # Upload to Spotify
  return upload_playlist_image(sp, playlist_id, jpeg_path)


def apply_template_variables(text: str, job: Dict[str, Any], playlist_name: str) -> str:
  """
  Apply template variables to text, similar to generate_playlist_name.

  Args:
      text: Text with template variables
      job: Job configuration
      playlist_name: Name of the playlist

  Returns:
      Text with template variables replaced
  """
  import datetime

  now = datetime.datetime.now()

  # Calculate date range for consistency with playlist naming
  cutoff_date = now - datetime.timedelta(days=7)
  date_range_start = cutoff_date
  date_range_end = now
  date_range_days = (date_range_end - date_range_start).days

  # Basic date variables
  text = text.replace('{playlist_name}', playlist_name)
  text = text.replace('{job_name}', job.get('name', 'unknown'))
  text = text.replace('{date}', now.strftime('%Y-%m-%d'))
  text = text.replace('{year}', str(now.year))
  text = text.replace('{month}', now.strftime('%B'))
  text = text.replace('{week_num}', str(now.isocalendar()[1]))

  # Date range variables (matching the playlist naming system)
  text = text.replace('{date_range_start_date}',
                      date_range_start.strftime('%Y-%m-%d'))
  text = text.replace('{date_range_end_date}',
                      date_range_end.strftime('%Y-%m-%d'))
  text = text.replace('{date_range_days}', str(date_range_days))

  return text
