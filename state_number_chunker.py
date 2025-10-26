#!/usr/bin/env python3
"""
State Number Chunker (merged with Streamlit UI)

Run modes:
 - CLI / normal:  python state_number_chunker.py --self-test
 - Streamlit UI:  streamlit run state_number_chunker.py

Features in UI:
 - Upload CSV or XLSX
 - Auto-detect states; multi-select which states to include
 - Set chunk size
 - Option to include 'rest' (unselected states) as separate chunk
 - Balanced chunking by round-robin across (state, area) groups
 - Download all chunk files as a ZIP directly from the browser

Dependencies:
  pip install pandas openpyxl streamlit

"""
from __future__ import annotations
import argparse
import io
import zipfile
import tempfile
import os
import sys
import re
from collections import defaultdict, deque
from math import ceil
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import pandas as pd
except Exception:
    print('Missing pandas. Install: pip install pandas openpyxl')
    raise

# Streamlit import is optional for CLI mode
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except Exception:
    STREAMLIT_AVAILABLE = False

PHONE_RE = re.compile(r"[^0-9]")


def normalize_number(num_str: Optional[str]) -> Optional[str]:
    if num_str is None:
        return None
    s = PHONE_RE.sub('', str(num_str))
    if not s:
        return None
    if len(s) > 10 and s.startswith('1'):
        s = s[1:]
    if len(s) >= 10:
        s = s[-10:]
    return s if len(s) >= 3 else None


def extract_area_code(num: Optional[str]) -> Optional[str]:
    nn = normalize_number(num)
    return nn[:3] if nn else None


def read_input_file(path_or_buffer) -> pd.DataFrame:
    """Read CSV/XLSX from a path or bytes buffer. Return dataframe with State and Number columns (as strings)."""
    if isinstance(path_or_buffer, (str, Path)):
        p = Path(path_or_buffer)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
        ext = p.suffix.lower()
        if ext in ['.xls', '.xlsx']:
            df = pd.read_excel(p, dtype=str)
        else:
            df = pd.read_csv(p, dtype=str)
    else:
        # assume file-like (BytesIO) from Streamlit uploader
        try:
            df = pd.read_csv(path_or_buffer, dtype=str)
        except Exception:
            path_or_buffer.seek(0)
            df = pd.read_excel(path_or_buffer, dtype=str)
    # Normalize expected columns
    if 'State' not in df.columns or 'Number' not in df.columns:
        # try lowercase or other names
        cols = {c.lower(): c for c in df.columns}
        if 'state' in cols and 'number' in cols:
            df = df.rename(columns={cols['state']: 'State', cols['number']: 'Number'})
        else:
            raise ValueError("Input must contain columns named 'State' and 'Number' (case-insensitive).")

    df = df[['State', 'Number']].copy()
    df['State'] = df['State'].astype(str).str.strip().str.upper()
    df['Number'] = df['Number'].astype(str).str.strip()
    df['AreaCode'] = df['Number'].apply(extract_area_code)
    return df


def balanced_round_robin_chunks(df: pd.DataFrame, selected_states: Optional[List[str]], chunk_size: int, include_rest: bool = True) -> List[pd.DataFrame]:
    """
    Create chunks balanced across (state, area) groups using round-robin assignment.
    Returns list of DataFrames (chunks).
    """
    # Filter selected states
    if selected_states:
        sel_set = {s.upper() for s in selected_states}
        sel_df = df[df['State'].isin(sel_set)].copy()
        rest_df = df[~df['State'].isin(sel_set)].copy()
    else:
        sel_df = df.copy()
        rest_df = pd.DataFrame(columns=df.columns)

    # Build queues per (state, area)
    groups = {}
    grouped = sel_df.groupby(['State', 'AreaCode'], sort=True)
    for (st, ac), g in grouped:
        # create deque of records (as dicts or DataFrame rows)
        groups[(st, ac if pd.notna(ac) else '')] = deque(g.to_dict('records'))

    total_records = sum(len(q) for q in groups.values())
    num_chunks = max(1, ceil(total_records / chunk_size))

    chunks = [ [] for _ in range(num_chunks) ]

    # Round-robin across group keys, assign one record at a time per group to current chunk, cycling
    group_keys = list(groups.keys())
    gi = 0
    ci = 0
    while group_keys:
        key = group_keys[gi % len(group_keys)]
        q = groups.get(key)
        if q and q:
            rec = q.popleft()
            chunks[ci].append(rec)
            ci = (ci + 1) % num_chunks
            # if this chunk is full, we keep distributing but final length may slightly exceed
        else:
            # remove empty queue
            group_keys.remove(key)
            # don't advance gi
            continue
        gi += 1

    # convert lists to DataFrames, drop empty chunks
    df_chunks = []
    for lst in chunks:
        if not lst:
            continue
        df_chunks.append(pd.DataFrame(lst))

    # If include_rest, append rest_df as its own chunk(s) (split into sizes of chunk_size)
    if include_rest and not rest_df.empty:
        rest_records = rest_df.to_dict('records')
        for i in range(0, len(rest_records), chunk_size):
            df_chunks.append(pd.DataFrame(rest_records[i:i+chunk_size]))

    return df_chunks


def make_zip_from_chunks(df_chunks: List[pd.DataFrame], base_name: str = 'chunks') -> Tuple[bytes, List[str]]:
    """Return ZIP bytes and list of filenames included."""
    mem = io.BytesIO()
    filenames = []
    with zipfile.ZipFile(mem, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, dfc in enumerate(df_chunks, start=1):
            name = f"{base_name}_chunk_{idx}.csv"
            filenames.append(name)
            csv_bytes = dfc.to_csv(index=False).encode('utf-8')
            zf.writestr(name, csv_bytes)
    mem.seek(0)
    return mem.read(), filenames


# ---------------- Streamlit UI ----------------

def run_streamlit_app():
    if not STREAMLIT_AVAILABLE:
        print('Streamlit is not installed in this environment. Install with: pip install streamlit')
        return

    st.set_page_config(page_title='State Number Chunker', layout='wide')
    st.title('State Number Chunker — Upload and create balanced chunks')

    with st.sidebar:
        st.markdown('**Settings**')
        chunk_size = st.number_input('Chunk size (rows)', min_value=1, value=10000, step=1)
        include_rest = st.checkbox('Include unselected states as separate "rest" chunks', value=True)
        run_demo = st.button('Run Demo Data')

    uploaded = st.file_uploader('Upload CSV or Excel file (must have State and Number columns)', type=['csv','xlsx','xls'])

    df = None
    if run_demo:
        # create demo dataframe
        demo = {
            'State': ['CA','CA','TX','TX','NY','NY','FL','FL','GA','GA'],
            'Number': ['+1-213-555-0001','310-555-0002','5125550003','7135550004','7185550005','2125550006','3055550007','7865550008','4045550009','4705550010']
        }
        df = pd.DataFrame(demo)
        st.success('Demo data loaded into table below.')

    if uploaded is not None:
        try:
            df = read_input_file(uploaded)
            st.success('File loaded successfully.')
        except Exception as e:
            st.error(f'Failed to read file: {e}')
            st.stop()

    if df is None:
        st.info('Upload a file or run demo to begin.')
        return

    st.subheader('Preview of data (first 100 rows)')
    st.dataframe(df.head(100))

    # detect states
    states = sorted(df['State'].dropna().unique().tolist())
    selected_states = st.multiselect('Select states to include (leave empty to include all)', options=states, default=states)

    if st.button('Run Chunker'):
        with st.spinner('Creating chunks...'):
            try:
                df_chunks = balanced_round_robin_chunks(df, selected_states if selected_states else None, chunk_size=int(chunk_size), include_rest=include_rest)
                if not df_chunks:
                    st.warning('No chunks were created (no data after filtering).')
                else:
                    zip_bytes, filenames = make_zip_from_chunks(df_chunks, base_name='chunks')
                    st.success(f'Created {len(filenames)} chunk files.')
                    st.download_button(label='Download all chunks (ZIP)', data=zip_bytes, file_name='chunks.zip', mime='application/zip')
                    # show small table of produced chunks
                    summary = [{'file': fn, 'rows': int(pd.read_csv(io.BytesIO(zipfile.ZipFile(io.BytesIO(zip_bytes)).read(fn))).shape[0])} for fn in filenames]
                    st.table(pd.DataFrame(summary))
            except Exception as e:
                st.error(f'Chunking failed: {e}')


# ---------------- CLI helpers ----------------

def cli_self_test():
    print('Running CLI self-test...')
    demo = {
        'State': ['CA','CA','TX','TX','NY','NY','FL','FL','GA','GA'],
        'Number': ['+1-213-555-0001','310-555-0002','5125550003','7135550004','7185550005','2125550006','3055550007','7865550008','4045550009','4705550010']
    }
    df = pd.DataFrame(demo)
    df.to_csv('demo_numbers.csv', index=False)
    print('Demo file written to demo_numbers.csv')
    df2 = read_input_file('demo_numbers.csv')
    chunks = balanced_round_robin_chunks(df2, selected_states=None, chunk_size=3)
    for i, c in enumerate(chunks, start=1):
        print(f'Chunk {i}: {len(c)} rows')
    zipb, names = make_zip_from_chunks(chunks, base_name='cli_chunks')
    with open('cli_chunks.zip','wb') as f:
        f.write(zipb)
    print('Wrote cli_chunks.zip with', len(names), 'files')


# ---------------- Main ----------------

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(prog='state_number_chunker', description='State Number Chunker with optional Streamlit UI')
    parser.add_argument('--self-test', action='store_true', help='Run quick CLI demo and exit')
    parser.add_argument('--run-ui', action='store_true', help='Run Streamlit UI (requires streamlit). Use: streamlit run state_number_chunker.py')
    parser.add_argument('--input', '-i', help='Input CSV/XLSX file for CLI mode')
    parser.add_argument('--chunk-size', '-c', type=int, default=10000, help='Rows per chunk for CLI mode')
    parser.add_argument('--states', '-s', default='', help='Comma-separated state initials to include for CLI mode')
    parser.add_argument('--outdir', '-o', default='./chunks', help='Output directory for CLI chunks')
    args = parser.parse_args(argv)

    if args.self_test:
        cli_self_test()
        return

    if args.run_ui:
        if not STREAMLIT_AVAILABLE:
            print('Streamlit is not available. Install with: pip install streamlit')
            return
        run_streamlit_app()
        return

    if args.input:
        df = read_input_file(args.input)
        states = [s.strip().upper() for s in args.states.split(',')] if args.states else None
        chunks = balanced_round_robin_chunks(df, selected_states=states, chunk_size=args.chunk_size, include_rest=True)
        files = []
        Path(args.outdir).mkdir(parents=True, exist_ok=True)
        for idx, c in enumerate(chunks, start=1):
            fname = Path(args.outdir) / f'chunk_{idx}.csv'
            c.to_csv(fname, index=False)
            files.append(str(fname))
        print(f'Wrote {len(files)} chunk files to {Path(args.outdir).resolve()}')
        return

    # Default behavior: no args -> if streamlit available, run UI; otherwise, run CLI self-test
    if STREAMLIT_AVAILABLE:
        run_streamlit_app()
    else:
        print('No arguments provided. Running CLI self-test (no streamlit).')
        cli_self_test()


if __name__ == '__main__':
    main()
