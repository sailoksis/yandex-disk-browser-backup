"""Yandex Disk UI backup. Python 3.10+, playwright. No REST API.
Commands: login, scan, backup, retry-failed, retry-preview, verify, status, export, confirm-empty.
Scan is resumable; scan --fresh rebuilds catalog after confirmation via explicit flag.
"""
import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import shutil
import sqlite3
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

from playwright.async_api import async_playwright, Error as PWError, TimeoutError as PWTimeout

BASE = 'https://disk.yandex.ru/client/disk'
ROOT = Path(os.getenv('BACKUP_DIR', r'E:\YandexBackup'))
PROFILE = Path(os.getenv('BROWSER_PROFILE', '.yandex-browser-profile')).resolve()
DB = ROOT / '_backup.sqlite3'
START_MS = int(os.getenv('DOWNLOAD_START_TIMEOUT_MS', '5000'))
COMPLETE_S = int(os.getenv('DOWNLOAD_COMPLETE_TIMEOUT_SECONDS', '1800'))
DELAY = float(os.getenv('DOWNLOAD_DELAY_SECONDS', '0.2'))
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp', '.tif', '.tiff', '.heic', '.avif'}


def stamp():
    return datetime.now(timezone.utc).isoformat()


def safe(name):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name).rstrip(' .') or '_'
    if re.match(r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)', name, re.I):
        name = '_' + name
    return name


def cloud_path(parts):
    return '/'.join(parts)


def local_path(parts):
    return ROOT.joinpath(*(safe(x) for x in parts))


def folder_url(parts):
    return BASE + ('/' + '/'.join(quote(x, safe='') for x in parts) if parts else '')


def canonical(url):
    parsed = urlsplit(str(url))
    if parsed.scheme != 'https' or parsed.netloc.lower() != 'disk.yandex.ru':
        return None
    return unquote(parsed.path).rstrip('/')


def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


class Store:
    def __init__(self):
        ROOT.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(DB)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA busy_timeout=5000')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS folders (
          path TEXT PRIMARY KEY, parts TEXT NOT NULL, url TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending', item_count INTEGER,
          error TEXT, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS files (
          path TEXT PRIMARY KEY, parts TEXT NOT NULL, parent TEXT NOT NULL,
          name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
          bytes INTEGER, sha256 TEXT, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL,
          kind TEXT NOT NULL, path TEXT, details TEXT);
        CREATE INDEX IF NOT EXISTS idx_folders_status ON folders(status);
        CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
        ''')
        self.db.commit()
        self.db.execute("UPDATE folders SET status='pending' WHERE status='scanning'")
        self.db.execute("UPDATE files SET status='pending' WHERE status='downloading'")
        self.db.commit()

    def event(self, kind, path='', details=''):
        self.db.execute('INSERT INTO events(at,kind,path,details) VALUES(?,?,?,?)',
                        (stamp(), kind, path, str(details)[:4000]))
        self.db.commit()

    def add_folder(self, parts):
        self.db.execute('INSERT OR IGNORE INTO folders(path,parts,url,status,updated_at) VALUES(?,?,?,?,?)',
                        (cloud_path(parts), json.dumps(parts, ensure_ascii=False), folder_url(parts), 'pending', stamp()))
        self.db.commit()

    def next_folder(self):
        return self.db.execute("SELECT * FROM folders WHERE status='pending' ORDER BY LENGTH(path),path LIMIT 1").fetchone()

    def scanned(self):
        return self.db.execute("SELECT COUNT(*) FROM folders WHERE status!='done'").fetchone()[0] == 0 and self.db.execute('SELECT COUNT(*) FROM folders').fetchone()[0] > 0

    def save_folder_listing(self, parts, entries):
        """Atomic: discovered children and folder completion commit together."""
        key = cloud_path(parts)
        at = stamp()
        with self.db:
            for entry in entries:
                child = [*parts, entry['name']]
                path = cloud_path(child)
                if entry['type'] == 'dir':
                    self.db.execute('INSERT OR IGNORE INTO folders(path,parts,url,status,updated_at) VALUES(?,?,?,?,?)',
                                    (path, json.dumps(child, ensure_ascii=False), folder_url(child), 'pending', at))
                else:
                    self.db.execute('''INSERT OR IGNORE INTO files(path,parts,parent,name,status,updated_at)
                                       VALUES(?,?,?,?,?,?)''',
                                    (path, json.dumps(child, ensure_ascii=False), key, entry['name'], 'pending', at))
            self.db.execute("UPDATE folders SET status='done', item_count=?, error=NULL, updated_at=? WHERE path=?",
                            (len(entries), at, key))

    def file_status(self, path, status, *, size=None, digest=None, error=None, attempt=False):
        with self.db:
            self.db.execute('''UPDATE files SET status=?, bytes=COALESCE(?,bytes),
                 sha256=COALESCE(?,sha256), error=?, attempts=attempts+?, updated_at=? WHERE path=?''',
                 (status, size, digest, error, int(attempt), stamp(), path))

    def counts(self):
        return {
            'folders': dict(self.db.execute('SELECT status,COUNT(*) FROM folders GROUP BY status').fetchall()),
            'files': dict(self.db.execute('SELECT status,COUNT(*) FROM files GROUP BY status').fetchall()),
        }

    def export(self):
        out = ROOT / '_backup_inventory.csv'
        tmp = out.with_suffix('.csv.tmp')
        with tmp.open('w', encoding='utf-8-sig', newline='') as f:
            w = csv.writer(f)
            w.writerow(['type','cloud_path','cloud_folder_url','local_path','status','local_exists','local_bytes','sha256','error'])
            for row in self.db.execute('SELECT * FROM folders ORDER BY path'):
                parts = json.loads(row['parts'])
                p = local_path(parts)
                w.writerow(['dir', row['path'], row['url'], str(p), row['status'], p.is_dir(), '', '', row['error'] or ''])
            for row in self.db.execute('SELECT * FROM files ORDER BY path'):
                parts = json.loads(row['parts'])
                p = local_path(parts)
                w.writerow(['file', row['path'], folder_url(parts[:-1]), str(p), row['status'],
                            p.is_file(), p.stat().st_size if p.is_file() else '', row['sha256'] or '', row['error'] or ''])
        tmp.replace(out)
        summary = {'updated_at': stamp(), 'scan_complete': self.scanned(), **self.counts()}
        summary_path = ROOT / '_backup_summary.json'
        tmp = summary_path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        tmp.replace(summary_path)
        print('[EXPORT]', out)


class Browser:
    def __init__(self, ctx):
        self.ctx = ctx
        self.page = ctx.pages[0] if ctx.pages else None

    async def alive(self):
        if self.page is None or self.page.is_closed():
            pages = [p for p in self.ctx.pages if not p.is_closed()]
            if not pages:
                raise RuntimeError('Все вкладки браузера закрыты')
            self.page = pages[0]
        return self.page

    async def captcha(self):
        p = await self.alive()
        if re.search(r'showcaptcha|captcha\.yandex|smartcaptcha|/challenge(?:[/?#]|$)', p.url, re.I):
            return True
        try:
            return await p.evaluate('''() => {
              const vis=e=>!!e.getClientRects().length;
              return ['iframe[src*="captcha"]','.SmartCaptcha','.smart-captcha','[data-testid*="captcha"]']
                .some(s=>[...document.querySelectorAll(s)].some(vis)) ||
                /подтвердите, что вы не робот|докажите, что вы не робот|verify you are human/i
                .test((document.body?.innerText||'').slice(0,3000));
            }''')
        except PWError:
            return False

    async def wait_captcha(self):
        if await self.captcha():
            print('[CAPTCHA] Пройдите проверку вручную в Chrome.')
            while await self.captcha():
                await asyncio.sleep(2)
            await asyncio.sleep(2)

    async def visit(self, parts):
        p = await self.alive()
        expected = folder_url(parts)
        for attempt in range(3):
            try:
                await self.wait_captcha()
                await p.goto(expected, wait_until='domcontentloaded', timeout=60000)
                await self.wait_captcha()
                if canonical(p.url) != canonical(expected):
                    raise RuntimeError(f'Неверный URL: {p.url} != {expected}')
                # An empty folder has no .listing-item nodes. Wait for either
                # items or an explicit empty-state UI; never infer emptiness
                # from a timeout alone.
                await p.wait_for_function(r'''() => {
                  const items = document.querySelectorAll('.listing-item_type_dir,.listing-item_type_file');
                  if (items.length) return true;
                  const text = (document.body?.innerText || '').toLowerCase();
                  const emptyText = /папка пуста|папка пустая|эта папка пуста|в этой папке (пока )?ничего нет|здесь (пока )?ничего нет|в папке нет файлов|нет файлов|folder is empty|this folder is empty|no files here/.test(text);
                  const selectors = [
                    '[data-testid="listing-folder-stub"]', '.ListingFolderStub__stub',
                    '.listing-stub', '.listing__empty', '.listing-empty',
                    '.empty-folder', '[data-testid="empty-folder"]',
                    '[class*="empty-state"]', '[class*="emptyState"]'
                  ];
                  const visible = el => !!el.getClientRects().length;
                  return emptyText || selectors.some(sel =>
                    [...document.querySelectorAll(sel)].some(visible));
                }''', timeout=16000)
                await asyncio.sleep(0.5)
                if canonical(p.url) != canonical(expected):
                    raise RuntimeError('URL изменился после загрузки')
                return p
            except (PWTimeout, PWError, RuntimeError) as e:
                if p.is_closed():
                    raise RuntimeError('Chrome закрылся') from e
                if attempt == 2:
                    # A screenshot helps identify the actual Yandex empty-state
                    # without ever silently marking a failed page as empty.
                    try:
                        diag = ROOT / '_scan_diagnostics'
                        diag.mkdir(parents=True, exist_ok=True)
                        import hashlib as _hashlib
                        label = _hashlib.sha256(expected.encode('utf-8')).hexdigest()[:12]
                        shot = diag / f'{label}.png'
                        await p.screenshot(path=str(shot), full_page=False, timeout=5000)
                        print('[SCAN DEBUG] Скриншот:', shot)
                    except Exception:
                        pass
                    raise RuntimeError(f'Папка не открылась: {expected}: {e}') from e
                await asyncio.sleep(2)
        raise RuntimeError('Не удалось открыть папку')

    async def items(self):
        p = await self.alive()
        return await p.evaluate('''async () => {
          const sleep=ms=>new Promise(r=>setTimeout(r,ms));
          const seen=new Map();
          const first=document.querySelector('.listing-item');
          if(!first)return [];
          let sc=first.parentElement;
          while(sc && sc!==document.body && sc!==document.documentElement){
            const s=getComputedStyle(sc);
            if(/(auto|scroll)/.test(s.overflowY)&&sc.scrollHeight>sc.clientHeight+20)break;
            sc=sc.parentElement;
          }
          if(!sc || sc===document.body || sc===document.documentElement)sc=document.scrollingElement;
          const harvest=()=>{
            for(const el of document.querySelectorAll('.listing-item_type_dir,.listing-item_type_file')){
              const name=el.querySelector('.listing-item__title[aria-label]')?.getAttribute('aria-label');
              if(!name)continue;
              const type=el.classList.contains('listing-item_type_dir')?'dir':'file';
              seen.set(type+':'+name,{type,name});
            }
          };
          sc.scrollTop=0;await sleep(300);let stagnant=0,prev=-1;
          for(let i=0;i<1500;i++){
            harvest();
            const next=Math.min(sc.scrollTop+Math.max(300,sc.clientHeight*.7),sc.scrollHeight-sc.clientHeight);
            sc.scrollTop=Math.max(0,next);await sleep(220);harvest();
            if(sc.scrollTop===prev || sc.scrollTop>=sc.scrollHeight-sc.clientHeight-3)stagnant++;
            else stagnant=0;
            if(stagnant>=5)break;
            prev=sc.scrollTop;
          }
          sc.scrollTop=0;return [...seen.values()];
        }''')

    async def tile(self, name):
        p = await self.alive()
        selector = '.listing-item_type_file:has(.listing-item__title[aria-label=' + json.dumps(name, ensure_ascii=False) + '])'
        loc = p.locator(selector).first
        if await loc.count() and await loc.is_visible():
            return loc
        await p.evaluate('''async name=>{
          const sleep=ms=>new Promise(r=>setTimeout(r,ms));
          const first=document.querySelector('.listing-item');let sc=first?.parentElement;
          while(sc && sc!==document.body && sc!==document.documentElement){
            const s=getComputedStyle(sc);
            if(/(auto|scroll)/.test(s.overflowY)&&sc.scrollHeight>sc.clientHeight+20)break;
            sc=sc.parentElement;
          }
          if(!sc || sc===document.body || sc===document.documentElement)sc=document.scrollingElement;
          sc.scrollTop=0;
          for(let i=0;i<1500;i++){
            const el=[...document.querySelectorAll('.listing-item_type_file')]
              .find(e=>e.querySelector('.listing-item__title')?.getAttribute('aria-label')===name);
            if(el){el.scrollIntoView({block:'center'});return;}
            const old=sc.scrollTop;sc.scrollTop+=Math.max(300,sc.clientHeight*.7);
            await sleep(180);if(sc.scrollTop===old)break;
          }
        }''', name)
        await loc.wait_for(state='visible', timeout=8000)
        return loc


def validate_entries(entries):
    local_names = {}
    cloud_names = set()
    for entry in entries:
        name, kind = entry['name'], entry['type']
        if not name or name in ('.','..') or kind not in ('dir','file'):
            raise RuntimeError('Некорректный элемент в HTML-каталоге')
        # Windows filesystem case-insensitivity and sanitization collisions.
        normalized = safe(name).casefold()
        if normalized in local_names and local_names[normalized] != name:
            raise RuntimeError(f'Конфликт локальных имён: {local_names[normalized]!r} / {name!r}')
        local_names[normalized] = name
        if name in cloud_names:
            raise RuntimeError(f'Дублирующееся имя в папке: {name!r}')
        cloud_names.add(name)


async def scan(browser, store, max_folders=0):
    store.add_folder([])
    done = 0
    while (row := store.next_folder()) is not None:
        if max_folders and done >= max_folders:
            break
        parts = json.loads(row['parts'])
        key = row['path']
        print('[SCAN]', key or '/', row['url'])
        with store.db:
            store.db.execute("UPDATE folders SET status='scanning',updated_at=? WHERE path=?", (stamp(), key))
        try:
            await browser.visit(parts)
            entries = await browser.items()
            validate_entries(entries)
            store.save_folder_listing(parts, entries)
            done += 1
            print('[FOUND]', len(entries), 'элементов')
        except Exception as e:
            with store.db:
                store.db.execute("UPDATE folders SET status='error',error=?,updated_at=? WHERE path=?", (str(e), stamp(), key))
            store.event('scan_error', key, str(e))
            print('[SCAN ERROR]', key or '/', e)
        store.export()
    print('[SCAN RESULT]', store.counts(), 'scan_complete=', store.scanned())


def preview_path(target):
    # Separate from original; screenshots must never be mistaken for originals.
    return target.with_name(target.name + '.preview.png')


async def capture_image_preview(browser, parts, target):
    """Open Yandex photo viewer and capture displayed image pixels, not original bytes."""
    p = await browser.visit(parts[:-1])
    tile = await browser.tile(parts[-1])
    await tile.dblclick(timeout=10000)
    dialog = p.locator('[role="dialog"].slider')
    try:
        await dialog.wait_for(state='visible', timeout=12000)
        image = dialog.locator('.slider__item_active .scalable-preview__image').first
        await image.wait_for(state='visible', timeout=12000)
        # Yandex can render a visible placeholder <img> before its real pixels load,
        # and can replace the active image node during preview transitions.
        # Poll the CURRENT active node rather than awaiting events on a stale node.
        await p.wait_for_function("""() => {
          const dialog = document.querySelector('[role="dialog"].slider');
          const el = dialog?.querySelector('.slider__item_active .scalable-preview__image');
          return !!(el && el.complete && el.naturalWidth > 0 && el.naturalHeight > 0);
        }""", timeout=15000, polling=250)
        image = dialog.locator('.slider__item_active .scalable-preview__image').first
        await image.wait_for(state='visible', timeout=5000)
        # Element screenshot avoids browser chrome, sidebar and preview controls.
        destination = preview_path(target)
        temporary = destination.with_name(destination.name + '.part.png')
        try:
            await image.screenshot(path=str(temporary), animations='disabled', timeout=15000)
            if not temporary.is_file() or temporary.stat().st_size < 100:
                raise RuntimeError('Скриншот отсутствует или слишком мал')
            if destination.exists():
                raise RuntimeError(f'Превью уже существует: {destination}')
            temporary.replace(destination)
        finally:
            if temporary.exists():
                temporary.unlink()
        return destination
    finally:
        try:
            await p.keyboard.press('Escape')
            await dialog.wait_for(state='hidden', timeout=5000)
        except Exception:
            # Next operation always calls visit() to restore folder state.
            pass


async def download_one(browser, store, row):
    parts = json.loads(row['parts'])
    key = row['path']
    target = local_path(parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Never silently overwrite an existing file, even when hash differs.
    if target.exists():
        if not target.is_file() or target.stat().st_size == 0:
            store.file_status(key, 'suspect', error='Файл существует, но пустой или не является файлом')
            return
        digest = sha256(target)
        if row['sha256'] and row['sha256'] != digest:
            store.file_status(key, 'mismatch', error='SHA256 локального файла отличается от сохранённого')
            print('[MISMATCH]', key)
        else:
            store.file_status(key, 'existing' if not row['sha256'] else 'done',
                              size=target.stat().st_size, digest=digest)
            print('[EXISTS]', key)
        return
    tmp = target.with_name(target.name + '.part')
    if tmp.exists():
        tmp.unlink()  # A previous interrupted partial download is not a valid file.
    if shutil.disk_usage(ROOT).free < 100 * 1024 * 1024:
        raise RuntimeError('На диске осталось менее 100 МБ; загрузка остановлена')
    store.file_status(key, 'downloading', attempt=True)
    try:
        p = await browser.visit(parts[:-1])
        if canonical(p.url) != canonical(folder_url(parts[:-1])):
            raise RuntimeError('Адрес родительской папки не совпадает')
        tile = await browser.tile(parts[-1])
        print('[SELECT]', key, 'tile=', await tile.count())
        await tile.click(timeout=8000)
        bar = p.locator('.resources-action-bar_visible')
        await bar.wait_for(state='visible', timeout=8000)
        button = bar.get_by_role('button', name='Скачать', exact=True)
        await button.wait_for(state='visible', timeout=8000)
        print('[CLICK DOWNLOAD]', key, 'button=', await button.count())
        async with p.expect_download(timeout=START_MS) as info:
            await button.click(timeout=8000)
        download = await info.value
        if download.suggested_filename != parts[-1]:
            raise RuntimeError(f'Имя загрузки не совпадает: {download.suggested_filename!r}')
        await asyncio.wait_for(download.save_as(str(tmp)), timeout=COMPLETE_S)
        failure = await download.failure()
        if failure:
            raise RuntimeError(f'Ошибка загрузки Chrome: {failure}')
        if not tmp.is_file() or tmp.stat().st_size == 0:
            raise RuntimeError('Скачанный файл пуст или отсутствует')
        digest = sha256(tmp)
        size = tmp.stat().st_size
        if target.exists():
            raise RuntimeError('Целевой файл появился во время скачивания')
        tmp.replace(target)
        store.file_status(key, 'done', size=size, digest=digest)
        store.event('downloaded', key, f'{size} bytes, sha256={digest}')
        print('[OK]', key, size, 'байт')
        if DELAY:
            await asyncio.sleep(DELAY)
    except Exception as e:
        try:
            diag = ROOT / '_download_diagnostics'
            diag.mkdir(parents=True, exist_ok=True)
            label = hashlib.sha256(key.encode('utf-8')).hexdigest()[:12]
            p = await browser.alive()
            await p.screenshot(path=str(diag / f'{label}.png'), timeout=7000)
            (diag / f'{label}.json').write_text(json.dumps({
                'cloud_path': key, 'expected_folder_url': folder_url(parts[:-1]),
                'actual_url': p.url, 'error': str(e), 'at': stamp(),
                'selected_action_bar_count': await p.locator('.resources-action-bar_visible').count(),
                'download_button_count': await p.locator('.resources-action-bar_visible').get_by_role('button', name='Скачать', exact=True).count(),
            }, ensure_ascii=False, indent=2), encoding='utf-8')
            print('[DEBUG]', diag / f'{label}.png')
        except Exception as diag_error:
            print('[DEBUG ERROR]', diag_error)
        store.event('download_error', key, str(e))
        print('[FAILED DOWNLOAD]', key, e)
        if target.suffix.lower() in IMAGE_EXTS:
            try:
                preview = preview_path(target)
                if preview.is_file() and preview.stat().st_size > 100:
                    print('[PREVIEW EXISTS]', key, preview)
                else:
                    preview = await capture_image_preview(browser, parts, target)
                    print('[PREVIEW SAVED]', key, preview)
                store.file_status(key, 'preview', error='Оригинал не скачан: ' + str(e))
                store.event('preview_saved', key, str(preview))
            except Exception as preview_error:
                store.file_status(key, 'failed', error=f'Скачивание: {e}; скриншот: {preview_error}')
                store.event('preview_error', key, str(preview_error))
                print('[FAILED PREVIEW]', key, preview_error)
        else:
            store.file_status(key, 'failed', error=str(e))
    finally:
        if tmp.exists():
            tmp.unlink()


async def backup(browser, store, retry_only=False, limit=0):
    if not store.scanned():
        print('[BLOCKED] Сканирование не завершено полностью. Запустите scan и проверьте ошибки.')
        return
    # Normal backup never retries known failures or screenshot-only copies.
    # Explicit retry commands operate on their own status exclusively.
    statuses = ('preview',) if retry_only == 'preview' else (('failed',) if retry_only else ('pending', 'missing'))
    placeholders = ','.join('?' for _ in statuses)
    query = f'SELECT * FROM files WHERE status IN ({placeholders}) ORDER BY path'
    rows = store.db.execute(query, statuses).fetchall()
    if limit:
        rows = rows[:limit]
    print('[BACKUP MODE]', 'retry-preview' if retry_only == 'preview' else ('retry-failed' if retry_only else 'new-only'))
    print('[BACKUP] Задач:', len(rows))
    for row in rows:
        await download_one(browser, store, row)
    store.export()
    print('[BACKUP RESULT]', store.counts())


def verify(store):
    for row in store.db.execute('SELECT * FROM files ORDER BY path').fetchall():
        p = local_path(json.loads(row['parts']))
        if not p.is_file() or p.stat().st_size == 0:
            if row['status'] == 'preview':
                preview = preview_path(p)
                if preview.is_file() and preview.stat().st_size > 100:
                    continue
                store.file_status(row['path'], 'failed', error='Превью отсутствует; оригинал тоже отсутствует')
                continue
            # Keep failure history visible until a successful retry.
            if row['status'] != 'failed':
                store.file_status(row['path'], 'missing', error='Файл отсутствует или пустой')
            continue
        digest = sha256(p)
        if row['sha256'] and row['sha256'] != digest:
            store.file_status(row['path'], 'mismatch', error='SHA256 не совпадает')
        else:
            store.file_status(row['path'], 'done' if row['sha256'] else 'existing',
                              size=p.stat().st_size, digest=digest)
    store.export()
    print('[VERIFY RESULT]', store.counts())


async def main():
    parser = argparse.ArgumentParser(description='Яндекс Диск: SQLite + Playwright, без API')
    parser.add_argument('command', choices=['login','scan','backup','retry-failed','retry-preview','verify','status','export','confirm-empty'])
    parser.add_argument('--max-folders', type=int, default=0, help='Лимит папок для пробного сканирования')
    parser.add_argument('--limit', type=int, default=0, help='Лимит скачиваний для теста')
    parser.add_argument('--retry-scan-errors', action='store_true', help='Повторно проверить папки со статусом error')
    parser.add_argument('--path', help='Точный облачный путь для confirm-empty (без ведущего /)')
    args = parser.parse_args()
    store = Store()
    if args.command == 'confirm-empty':
        if args.path is None:
            parser.error('Для confirm-empty нужен --path "точный/путь"')
        row = store.db.execute('SELECT status FROM folders WHERE path=?', (args.path,)).fetchone()
        if row is None:
            parser.error('Папка не найдена в SQLite')
        if row['status'] != 'error':
            parser.error('Можно подтверждать только папки со статусом error')
        print('Подтверждение ПУСТОЙ папки:', args.path)
        print('Используй только если вручную открыл этот URL и убедился, что там нет файлов и папок.')
        answer = input('Напиши EMPTY для подтверждения: ').strip()
        if answer != 'EMPTY':
            print('Отменено; база не изменена.')
            return
        store.save_folder_listing(json.loads(store.db.execute('SELECT parts FROM folders WHERE path=?', (args.path,)).fetchone()['parts']), [])
        store.event('manual_empty_confirmed', args.path, 'Пользователь подтвердил пустую папку вручную')
        store.export()
        return
    if args.command in ('verify','status','export'):
        if args.command == 'verify':
            verify(store)
        elif args.command == 'export':
            store.export()
        print(json.dumps({'scan_complete': store.scanned(), **store.counts()}, ensure_ascii=False, indent=2))
        return
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            str(PROFILE), channel='chrome', headless=False, accept_downloads=True,
            viewport={'width': 1440, 'height': 900})
        browser = Browser(ctx)
        try:
            if args.command == 'login':
                p = await browser.alive()
                await p.goto(BASE, wait_until='domcontentloaded')
                await asyncio.to_thread(input, 'Войдите в Яндекс Диск, затем нажмите Enter: ')
            elif args.command == 'scan':
                if args.retry_scan_errors:
                    with store.db:
                        store.db.execute("UPDATE folders SET status='pending',error=NULL WHERE status='error'")
                await scan(browser, store, args.max_folders)
            else:
                await backup(browser, store, retry_only=('preview' if args.command == 'retry-preview' else args.command == 'retry-failed'), limit=args.limit)
        finally:
            store.export()
            await ctx.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('\n[STOP] Прервано. SQLite сохранила завершённые транзакции.')
