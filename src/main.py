import os
import unicodedata
from multiprocessing.pool import ThreadPool
from pathlib import Path

import ebooklib
import ftfy
from bs4 import BeautifulSoup
from ebooklib import epub
from google.cloud import texttospeech
from pydub import AudioSegment


def split_text(text: str, delimiters: list[str]) -> list[str]:
    delimiter = delimiters[0]
    text_parts = text.split(delimiter)
    if not delimiters[1:]:
        return text_parts
    else:
        return sum(
            [split_text(text_part, delimiters[1:]) for text_part in text_parts], []
        )


def combine_text(
    text_parts: list[str], delimiter: str, max_bytes: int = 1000
) -> list[str]:
    def utf8len(s):
        return len(s.encode("utf-8"))

    output_splits = []
    str_builder = []
    cur_size = 0
    for text_part in text_parts:
        part_size = utf8len(text_part)
        cur_size += part_size
        if cur_size > max_bytes:
            output_splits.append(delimiter.join(str_builder))
            str_builder = []
            cur_size = 0

        if part_size > max_bytes:
            print(f"this part is too large: {text_part}")

        str_builder.append(text_part.replace("\u3000", "。"))

    output_splits.append(delimiter.join(str_builder))
    print([len(part) for part in output_splits])
    return output_splits


def contains_chinese(s):
    for ch in s:
        try:
            if "CJK" in unicodedata.name(ch):
                return True
        except ValueError:
            continue
    return False


def get_epub_chapters(epub_path: str, header_delimiter: str):
    book = epub.read_epub(epub_path)

    chapters = []
    for chapter in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
        soup = BeautifulSoup(chapter.get_body_content(), "html.parser")

        header = header_delimiter.join(
            ftfy.fix_text(tag.get_text())
            for tag_name in (f"h{i}" for i in range(1, 7))
            for tag in soup.find_all(tag_name)
        )
        header = header or chapter.get_name()

        parts = []
        for tag in soup.find_all():
            p = ftfy.fix_text(tag.get_text())
            if contains_chinese(p):
                splits = split_text(p, ["。", "!", "?"])
                combined_splits = combine_text(splits, "。", 500)
                parts.extend(combined_splits)

        chapters.append((header, parts))
        # chapters[chapter.get_name()] = chapter.get_body_content().decode()

    return chapters


def gcp_text_to_speech(input_text: str, output_file_path: str, voice_params: dict):
    client = texttospeech.TextToSpeechClient()

    voice = texttospeech.VoiceSelectionParams(**voice_params)
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MP3,
        speaking_rate=0.8,
    )

    synthesis_input = texttospeech.SynthesisInput(text=input_text)

    response = client.synthesize_speech(
        input=synthesis_input, voice=voice, audio_config=audio_config
    )

    with open(output_file_path, "wb") as out:
        out.write(response.audio_content)
        print(f'Audio content written to file "{output_file_path}"')

    return output_file_path


def main(epub_path: str, output_dir: str, voice_params: dict):
    chapters = get_epub_chapters(epub_path, "，")

    pool = ThreadPool(3)
    output_file_paths = []
    for i, (header, content_parts) in enumerate(chapters):
        _output_dir: Path = Path(output_dir) / f"{i}_{header}"
        _output_dir.mkdir(parents=True, exist_ok=True)

        def _process(x):
            j, part = x
            output_file_path = str(_output_dir / f"chunk_{j}.mp3")
            # return output_file_path
            try:
                return gcp_text_to_speech(part, output_file_path, voice_params)
            except:
                print(part)
                raise

        output_file_paths.append((header, pool.map(_process, enumerate(content_parts))))

    for i, (header, _output_file_paths) in enumerate(output_file_paths):
        try:
            combined_mp3 = sum(AudioSegment.from_mp3(p) for p in _output_file_paths)
            combined_mp3.export(Path(output_dir) / f"{i}_{header}.mp3", format="mp3")
        except Exception as e:
            print(header)
            print(e)


if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./path-to-credentials.json"

    epub_path = "path-to-book.epub"
    output_dir = "./path/to/dir"

    voice_params = dict(
        language_code="yue-HK",
        name="yue-HK-Standard-B",
    )

    main(epub_path, output_dir, voice_params)
