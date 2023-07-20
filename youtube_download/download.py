import os
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from tqdm import tqdm
from shutil import copyfile
import tempfile
import traceback


# if "Error: Unable to extract uploader id" occurs:
# execute the following
# python3 -m pip install --force-reinstall https://github.com/yt-dlp/yt-dlp/archive/master.tar.gz
# and import yt_dlp as youtube_dl

import yt_dlp as youtube_dl
from moviepy.audio.io.AudioFileClip import AudioFileClip


def download_youtube_audio(id, start_sec=0, end_sec=None, dir_save='./', 
                   fps=16000, nbytes=2, mono=False, prefix='AudioSet', suffix='wav', verbose=False):
    '''
    download YouTube audio 
    '''
    if not os.path.exists(dir_save):
        os.makedirs(dir_save, exist_ok=True)
    link = f'https://youtu.be/{id}'
    ydl_opts = {'format': 'bestaudio'}
    path = None
    with tempfile.TemporaryDirectory() as temp_dir:
        cwd = os.getcwd()
        os.chdir(temp_dir)
        stdout = sys.stdout
        nullout = open(os.devnull, 'w')
        if not verbose:
            sys.stdout = nullout
        try:            
            with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                ydl.download([link])
            temp_path = os.path.join(temp_dir, os.listdir(temp_dir)[0])
        except Exception as ex:
            sys.stdout = stdout
            print(ex)
            #traceback.print_exc()
        else:            
            os.chdir(cwd)
            try:
                if prefix is not None and len(prefix) > 0:
                    filename = f'{prefix}.{id}.{suffix}'
                else:
                    filename = f'{id}.{suffix}'
                path = os.path.join(dir_save, filename)
                audio_clip = AudioFileClip(temp_path)
                if start_sec > 0 or end_sec is not None:
                    audio_clip = audio_clip.subclip(t_start=start_sec, t_end=end_sec)
                if mono:
                    audio_clip.write_audiofile(path, fps=fps, nbytes=nbytes, ffmpeg_params=['-ac', '1'])
                else:
                    audio_clip.write_audiofile(path, fps=fps, nbytes=nbytes)
            except Exception as ex1:
                sys.stdout = stdout
                print(ex1)
                #traceback.print_exc()
        finally:            
            os.chdir(cwd)
            sys.stdout = stdout
            nullout.close()
    return path
        


def download_youtube_audio_many(ids_starts_ends, dir_save='./', 
                   fps=16000, nbytes=2, mono=False, prefix='AudioSet', suffix='wav', 
                   max_count=None, num_workers=8, verbose=False):
    '''
    ids_starts_ends = [(id, start_sec, end_sec)]
    '''
    if num_workers > 1:
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            jobs = []
            count = 0
            for id, start_sec, end_sec in ids_starts_ends:
                job = executor.submit(download_youtube_audio, id=id, start_sec=start_sec, end_sec=end_sec, dir_save=dir_save, 
                                       fps=fps, nbytes=nbytes, mono=mono, prefix=prefix, suffix=suffix, verbose=verbose)
                jobs.append((id, job))
                count += 1
                if max_count is not None and count >= max_count:
                    break
            results = {id:job.result() for id, job in tqdm(jobs)}            
    else:
        results
        count = 0
        for id, start_sec, end_sec in ids_starts_ends:
            path = download_youtube_audio(id, start_sec=start_sec, end_sec=end_sec, dir_save=dir_save, 
                                          fps=fps, nbytes=nbytes, mono=mono, prefix=prefix, suffix=suffix, verbose=verbose)
            results[id] = path
            count += 1
            if max_count is not None and count >= max_count:
                break
    return results
