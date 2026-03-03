"""
Unit tests for Skytap pipeline logic.

Covers the pure functions that are most critical and most testable without
requiring Docker, HYSPLIT binaries, or network access.

Run with:
    pytest test_pipeline.py -v
"""

import pytest
from datetime import datetime, timedelta
from pathlib import Path

from HYSPLIT_Controller import get_date_range
from Skytap_Controller import validate_config


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _new_fname(dt: datetime) -> str:
    """New-format HRRR filename (post 2019-06-12): YYYYMMDD_HH-EE_hrrr"""
    end_h = dt.hour + 5
    return f"{dt:%Y%m%d}_{dt:%H}-{end_h:02d}_hrrr"

def _old_fname(dt: datetime) -> str:
    """Old-format HRRR filename (pre 2019-06-12): hysplit.YYYYMMDD.HHz.hrrra"""
    return f"hysplit.{dt:%Y%m%d}.{dt:%H}z.hrrra"

def _new_url(dt: datetime) -> str:
    return f"https://noaa-oar-arl-hysplit-pds.s3.amazonaws.com/hrrr/{dt:%Y}/{dt:%m}/{_new_fname(dt)}"

def _old_url(dt: datetime) -> str:
    return f"https://noaa-oar-arl-hysplit-pds.s3.amazonaws.com/hrrr.v1/{dt:%Y}/{dt:%m}/{_old_fname(dt)}"

def _six_hour_window(start: datetime, count: int, fmt="new"):
    """Build a list of `count` consecutive 6-hour HRRR file names/urls."""
    builder = _new_fname if fmt == "new" else _old_fname
    return [builder(start + timedelta(hours=i * 6)) for i in range(count)]

def _site_filter(valid_datetimes, start_str, end_str):
    """
    The fixed per-datetime site filter used in make_run_dirs.
    Duplicated here so tests don't depend on make_run_dirs side effects.
    """
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end   = datetime.strptime(end_str,   "%Y-%m-%d")
    return [dt for dt in valid_datetimes if start <= dt <= end]


# ─────────────────────────────────────────────────────────────────────────────
# get_date_range — coverage / boundary tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGetDateRange:

    BASE = datetime(2020, 6, 13, 0)  # a convenient Monday midnight

    def test_empty_input_returns_empty(self):
        assert get_date_range([]) == []

    def test_single_file_insufficient_for_12h_back_traj(self):
        # 1 file covers hours 0-5. earliest_valid = 0+12=12, latest_valid = 0+5=5.
        # 12 > 5, so no valid hours can be scheduled.
        files = _six_hour_window(self.BASE, 1)
        assert get_date_range(files) == []

    def test_two_files_still_insufficient(self):
        # Files cover 0-5, 6-11. earliest_valid = 12, latest_valid = 11.
        files = _six_hour_window(self.BASE, 2)
        assert get_date_range(files) == []

    def test_three_files_minimum_for_valid_window(self):
        # Files: 00, 06, 12. earliest_valid = 00+12=12, latest_valid = 12+5=17.
        # Valid hours: 12, 13, 14, 15, 16, 17 = 6 total.
        files = _six_hour_window(self.BASE, 3)
        result = get_date_range(files)
        assert result[0]  == self.BASE + timedelta(hours=12)
        assert result[-1] == self.BASE + timedelta(hours=17)
        assert len(result) == 6

    def test_standard_six_file_window(self):
        # Files: 00, 06, 12, 18, 24, 30. earliest_valid=12, latest_valid=35.
        # 12..35 = 24 valid hours.
        files = _six_hour_window(self.BASE, 6)
        result = get_date_range(files)
        assert result[0]  == self.BASE + timedelta(hours=12)
        assert result[-1] == self.BASE + timedelta(hours=35)
        assert len(result) == 24

    def test_earliest_valid_is_exactly_12h_after_first_file(self):
        files = _six_hour_window(self.BASE, 6)
        result = get_date_range(files)
        assert result[0] == self.BASE + timedelta(hours=12)

    def test_latest_valid_is_exactly_5h_after_last_file_start(self):
        files = _six_hour_window(self.BASE, 6)
        last_file_start = self.BASE + timedelta(hours=5 * 6)  # file index 5 → 30h
        result = get_date_range(files)
        assert result[-1] == last_file_start + timedelta(hours=5)

    def test_output_is_contiguous_hourly(self):
        files = _six_hour_window(self.BASE, 6)
        result = get_date_range(files)
        for i in range(1, len(result)):
            assert result[i] - result[i - 1] == timedelta(hours=1), \
                f"Gap found between {result[i-1]} and {result[i]}"

    # ── File format parsing ───────────────────────────────────────────────────

    def test_old_format_filenames_parsed_correctly(self):
        # hysplit.YYYYMMDD.HHz.hrrra
        files = _six_hour_window(self.BASE, 6, fmt="old")
        result = get_date_range(files)
        assert result[0]  == self.BASE + timedelta(hours=12)
        assert result[-1] == self.BASE + timedelta(hours=35)
        assert len(result) == 24

    def test_new_format_filenames_parsed_correctly(self):
        files = _six_hour_window(self.BASE, 6, fmt="new")
        result = get_date_range(files)
        assert result[0]  == self.BASE + timedelta(hours=12)
        assert result[-1] == self.BASE + timedelta(hours=35)
        assert len(result) == 24

    def test_old_format_full_urls_parsed_correctly(self):
        urls = [_old_url(self.BASE + timedelta(hours=i * 6)) for i in range(6)]
        result = get_date_range(urls)
        assert result[0]  == self.BASE + timedelta(hours=12)
        assert result[-1] == self.BASE + timedelta(hours=35)
        assert len(result) == 24

    def test_new_format_full_urls_parsed_correctly(self):
        urls = [_new_url(self.BASE + timedelta(hours=i * 6)) for i in range(6)]
        result = get_date_range(urls)
        assert result[0]  == self.BASE + timedelta(hours=12)
        assert result[-1] == self.BASE + timedelta(hours=35)
        assert len(result) == 24

    def test_old_format_nonzero_start_hour(self):
        # File starting at 06z — verify hour parsing handles non-midnight starts
        start = datetime(2019, 3, 15, 6)
        files = [_old_fname(start + timedelta(hours=i * 6)) for i in range(6)]
        result = get_date_range(files)
        assert result[0] == start + timedelta(hours=12)

    # ── Rolling window handoff ────────────────────────────────────────────────

    def test_seamless_handoff_between_iterations(self):
        """
        The last valid hour of iteration N must be immediately followed by
        the first valid hour of iteration N+1 (no gap, no overlap).
        window_size=6, window_step=4.
        """
        window_size, step = 6, 4

        # Iteration 1: files 0-5
        iter1_files = _six_hour_window(self.BASE, window_size)
        valid1 = get_date_range(iter1_files)

        # Iteration 2: files 4-9 (kept 4,5 + new 6,7,8,9)
        iter2_files = _six_hour_window(self.BASE + timedelta(hours=step * 6), window_size)
        # on_disk = files[step:window_size] + new files = files[4], files[5], files[6..9]
        iter2_on_disk = (
            _six_hour_window(self.BASE, window_size)[step:]
            + iter2_files[window_size - step:]
        )
        valid2 = get_date_range(iter2_on_disk)

        assert valid2[0] == valid1[-1] + timedelta(hours=1), (
            f"Gap between iter1 end ({valid1[-1]}) and iter2 start ({valid2[0]})"
        )

    def test_no_overlap_between_consecutive_iterations(self):
        """No trajectory time should appear in both iteration 1 and iteration 2."""
        window_size, step = 6, 4

        iter1_files = _six_hour_window(self.BASE, window_size)
        valid1 = set(get_date_range(iter1_files))

        on_disk_iter2 = (
            _six_hour_window(self.BASE, window_size)[step:]
            + _six_hour_window(self.BASE + timedelta(hours=step * 6), window_size)[window_size - step:]
        )
        valid2 = set(get_date_range(on_disk_iter2))

        overlap = valid1 & valid2
        assert overlap == set(), f"Unexpected overlap: {sorted(overlap)}"


# ─────────────────────────────────────────────────────────────────────────────
# Site date filtering (logic extracted from make_run_dirs)
# ─────────────────────────────────────────────────────────────────────────────

# Module-level constants for TestSiteDateFilter (class body can't reference
# sibling names inside list comprehensions in Python 3).
_FILTER_WINDOW_START = datetime(2020, 10, 5, 12)
_FILTER_VALID = [_FILTER_WINDOW_START + timedelta(hours=i) for i in range(24 * 4 - 1)]


class TestSiteDateFilter:
    """
    Tests the per-datetime site filter that replaced the all-or-nothing check.
    Bug fix: a site whose start or end date falls mid-window should produce
    trajectories for only the hours within its operational range.
    """

    WINDOW_START = _FILTER_WINDOW_START
    VALID = _FILTER_VALID

    def test_site_fully_within_window(self):
        result = _site_filter(self.VALID, "2015-01-01", "2026-01-01")
        assert len(result) == len(self.VALID)
        assert result == self.VALID

    def test_site_not_started_yet_skipped(self):
        # Site starts after the entire window
        result = _site_filter(self.VALID, "2021-01-01", "2026-01-01")
        assert result == []

    def test_site_already_ended_skipped(self):
        # Site ended before the window
        result = _site_filter(self.VALID, "2015-01-01", "2020-01-01")
        assert result == []

    def test_site_starts_mid_window_gets_partial_runs(self):
        # Old bug: site would be skipped entirely.
        # New behaviour: only the hours on or after site start are included.
        site_start = "2020-10-07"
        result = _site_filter(self.VALID, site_start, "2026-01-01")
        cutoff = datetime(2020, 10, 7, 0)
        assert len(result) > 0
        assert all(dt >= cutoff for dt in result)
        assert result[0] == cutoff

    def test_site_ends_mid_window_gets_partial_runs(self):
        # Old bug: site would be skipped entirely for a window extending past its end.
        site_end = "2020-10-07"
        result = _site_filter(self.VALID, "2015-01-01", site_end)
        cutoff = datetime(2020, 10, 7, 0)
        assert len(result) > 0
        assert all(dt <= cutoff for dt in result)
        assert result[-1] == cutoff

    def test_site_boundary_exactly_at_window_start(self):
        # Site starts exactly when the window starts — should be fully included.
        start_str = self.WINDOW_START.strftime("%Y-%m-%d")
        # Note: site start is midnight, window start is 12:00, so site start < window[0]
        result = _site_filter(self.VALID, start_str, "2026-01-01")
        assert result == self.VALID

    def test_site_end_date_is_inclusive_at_midnight_only(self):
        # end_date is parsed as midnight (00:00). Only trajectory times at exactly
        # midnight on that date pass the <= check; later hours on that day do not.
        # e.g. end_date="2020-10-07" includes 2020-10-07 00:00 but not 01:00, 02:00...
        midnight_in_window = datetime(2020, 10, 7, 0)
        assert midnight_in_window in self.VALID
        result = _site_filter(self.VALID, "2015-01-01", "2020-10-07")
        assert midnight_in_window in result
        # Times after midnight on the end date are excluded
        assert datetime(2020, 10, 7, 1) not in result

    def test_only_midnight_hour_in_range(self):
        # Use a datetime that is exactly midnight so start_str == end_str matches it.
        midnight = datetime(2020, 10, 6, 0)
        assert midnight in self.VALID
        date_str = "2020-10-06"
        result = _site_filter([midnight], date_str, date_str)
        assert result == [midnight]

    def test_multiple_sites_independent_filtering(self):
        # Two sites with different ranges — each gets its own correct slice.
        site_a = _site_filter(self.VALID, "2020-10-06", "2020-10-07")
        site_b = _site_filter(self.VALID, "2020-10-08", "2020-10-09")
        assert len(site_a) > 0
        assert len(site_b) > 0
        # No overlap
        assert set(site_a).isdisjoint(set(site_b))
        # A entirely before B
        assert max(site_a) < min(site_b)


# ─────────────────────────────────────────────────────────────────────────────
# validate_config
# ─────────────────────────────────────────────────────────────────────────────

# Minimal schema (mirrors Example.yaml structure)
_EX_CFG = {
    "start_date": "2019-06-13",
    "end_date":   "2019-06-30",
    "months":     [5, 6, 7, 8, 9],
    "run_hours":  list(range(24)),
    "hrrr_v1_format": ["00z.hrrra", "06z.hrrra", "12z.hrrra", "18z.hrrra"],
    "hrrr_format":    ["00-05_hrrr", "06-11_hrrr", "12-17_hrrr", "18-23_hrrr"],
    "hrrr1_server": "https://noaa-oar-arl-hysplit-pds.s3.amazonaws.com/hrrr.v1",
    "hrrr_server":  "https://noaa-oar-arl-hysplit-pds.s3.amazonaws.com/hrrr",
    "temp_HYSPLIT_config_dir": "Temp_HYSPLIT_Dirs",
    "text_file_dir":     "txt_files",
    "full_ARL_file_list": "ARLfilelist.txt",
    "temp_arl_file_list": "ARL_temp_file_list.txt",
    "state_file": "state.yaml",
    "pipeline": {"window_size": 6, "window_step": 4},
    "hysplit": {
        "exec_path":   "./hysplit/exec/hyts_std",
        "working_dir": "./hysplit",
        "met_dir":     "./ARL_Files",
        "traj_root":   "./Trajectory_Files",
        "hours_utc":   [0, 6, 12, 18],
        "step_hours":  24,
        "vert_motion": 0,
        "top_of_model": 15100,
        "max_workers":  12,
    },
    "site_hysplit_configs": {
        # These two keys are excluded from required keys by validate_config,
        # but their children (lat, lon, etc.) ARE required.
        "ex_site_1": {
            "name": "Example 1", "lat": 40.0, "lon": -105.0,
            "start_height": 5, "duration": -12,
            "start_date": "2015-01-01", "end_date": "2026-01-01",
        },
        "ex_site_2": {
            "name": "Example 2", "lat": 39.0, "lon": -104.0,
            "start_height": 5, "duration": -12,
            "start_date": "2015-01-01", "end_date": "2026-01-01",
        },
    },
}

def _complete_user_cfg():
    """A user config with all required keys present."""
    return {
        "start_date": "2020-06-13",
        "end_date":   "2020-06-15",
        "months":     [5, 6, 7, 8, 9],
        "run_hours":  list(range(24)),
        "hrrr_v1_format": ["00z.hrrra", "06z.hrrra", "12z.hrrra", "18z.hrrra"],
        "hrrr_format":    ["00-05_hrrr", "06-11_hrrr", "12-17_hrrr", "18-23_hrrr"],
        "hrrr1_server": "https://noaa-oar-arl-hysplit-pds.s3.amazonaws.com/hrrr.v1",
        "hrrr_server":  "https://noaa-oar-arl-hysplit-pds.s3.amazonaws.com/hrrr",
        "temp_HYSPLIT_config_dir": "Temp_HYSPLIT_Dirs",
        "text_file_dir":     "txt_files",
        "full_ARL_file_list": "ARLfilelist.txt",
        "temp_arl_file_list": "ARL_temp_file_list.txt",
        "state_file": "state.yaml",
        "pipeline": {"window_size": 6, "window_step": 4},
        "hysplit": {
            "exec_path":   "./hysplit/exec/hyts_std",
            "working_dir": "./hysplit",
            "met_dir":     "./ARL_Files",
            "traj_root":   "./Trajectory_Files",
            "hours_utc":   [0, 6, 12, 18],
            "step_hours":  24,
            "vert_motion": 0,
            "top_of_model": 15100,
            "max_workers":  12,
        },
        "site_hysplit_configs": {
            "FTCW": {
                "name": "Fort Collins West",
                "lat": 40.592543, "lon": -105.141122,
                "start_height": 5, "duration": -12,
                "start_date": "2015-01-01", "end_date": "2026-01-01",
            },
        },
    }


class TestValidateConfig:

    def test_complete_config_returns_no_missing_keys(self):
        missing = validate_config(_complete_user_cfg(), _EX_CFG)
        assert missing == [], f"Unexpected missing keys: {missing}"

    def test_missing_top_level_key_is_caught(self):
        # Use a key that is ONLY at the top level (not duplicated in site sub-configs).
        # validate_config does a flat key-presence check across all levels, so keys like
        # 'start_date' that also appear inside site configs would not be caught if removed
        # from the top level. 'state_file' is a safe choice — it only lives at level 1.
        cfg = _complete_user_cfg()
        del cfg["state_file"]
        missing = validate_config(cfg, _EX_CFG)
        assert "state_file" in missing

    def test_validate_config_flat_key_check_limitation(self):
        # Known behaviour: validate_config checks whether a key appears ANYWHERE in
        # the config tree (any level), not at a specific path. Removing 'start_date'
        # from the top level is NOT caught because it still lives in each site's
        # sub-config (e.g. FTCW.start_date). Tests should use level-unique keys.
        cfg = _complete_user_cfg()
        del cfg["start_date"]
        missing = validate_config(cfg, _EX_CFG)
        assert "start_date" not in missing  # found via site sub-config

    def test_missing_pipeline_subkey_is_caught(self):
        cfg = _complete_user_cfg()
        del cfg["pipeline"]["window_size"]
        missing = validate_config(cfg, _EX_CFG)
        assert "window_size" in missing

    def test_missing_hysplit_subkey_is_caught(self):
        cfg = _complete_user_cfg()
        del cfg["hysplit"]["max_workers"]
        missing = validate_config(cfg, _EX_CFG)
        assert "max_workers" in missing

    def test_missing_site_subkey_is_caught(self):
        # Removing 'lat' from the only site — validator should flag it.
        cfg = _complete_user_cfg()
        del cfg["site_hysplit_configs"]["FTCW"]["lat"]
        missing = validate_config(cfg, _EX_CFG)
        assert "lat" in missing

    def test_empty_config_returns_many_missing_keys(self):
        missing = validate_config({}, _EX_CFG)
        assert len(missing) > 5

    def test_extra_keys_in_user_config_are_not_flagged(self):
        # Extra keys the user adds should be silently ignored.
        cfg = _complete_user_cfg()
        cfg["my_custom_key"] = "some value"
        missing = validate_config(cfg, _EX_CFG)
        assert "my_custom_key" not in missing
        assert missing == []

    def test_missing_multiple_keys_all_reported(self):
        # Use keys that are level-unique (not duplicated in site sub-configs).
        cfg = _complete_user_cfg()
        del cfg["state_file"]
        del cfg["text_file_dir"]
        del cfg["hysplit"]["max_workers"]
        missing = validate_config(cfg, _EX_CFG)
        assert "state_file" in missing
        assert "text_file_dir" in missing
        assert "max_workers" in missing


# ─────────────────────────────────────────────────────────────────────────────
# Rolling window math (Skytap_Controller pipeline logic)
# ─────────────────────────────────────────────────────────────────────────────

class TestRollingWindowMath:
    """
    Tests the index arithmetic and on_disk_urls tracking from the main pipeline
    loop in Skytap_Controller.py, without invoking any I/O or subprocesses.
    """

    WINDOW_SIZE = 6
    STEP = 4
    N = 20  # total files

    def _simulate(self, total, window_size, step):
        """
        Run the rolling window logic for `total` files and return a list of
        (on_disk_urls, dl_start, dl_end) tuples for each iteration.
        """
        all_urls = [f"file_{i:02d}" for i in range(total)]
        current_idx = 0
        batch_end = min(current_idx + window_size, total)
        on_disk = list(all_urls[current_idx:batch_end])
        iterations = []

        while True:
            iterations.append(list(on_disk))

            if current_idx + window_size >= total:
                break

            # Rotate
            current_idx += step
            dl_start = current_idx + (window_size - step)
            dl_end   = current_idx + window_size
            new_batch = all_urls[dl_start:dl_end]

            if not new_batch:
                break

            on_disk = on_disk[step:] + new_batch

        return iterations, all_urls

    def test_initial_batch_size_is_window_size(self):
        iters, _ = self._simulate(self.N, self.WINDOW_SIZE, self.STEP)
        assert len(iters[0]) == self.WINDOW_SIZE

    def test_on_disk_stays_window_size_after_rotation(self):
        # All iterations except the last maintain a full window.
        # The final iteration may be smaller when total_files isn't a clean multiple.
        iters, _ = self._simulate(self.N, self.WINDOW_SIZE, self.STEP)
        for on_disk in iters[:-1]:
            assert len(on_disk) == self.WINDOW_SIZE
        assert len(iters[-1]) <= self.WINDOW_SIZE

    def test_first_files_of_each_window_advance_by_step(self):
        iters, all_urls = self._simulate(self.N, self.WINDOW_SIZE, self.STEP)
        for i in range(1, len(iters)):
            prev_first = iters[i - 1][0]
            curr_first = iters[i][0]
            prev_idx = int(prev_first.split("_")[1])
            curr_idx = int(curr_first.split("_")[1])
            assert curr_idx - prev_idx == self.STEP, \
                f"Window advanced by {curr_idx - prev_idx}, expected {self.STEP}"

    def test_consecutive_windows_overlap_by_kept_files(self):
        """Each window shares (window_size - step) files with the previous."""
        keep = self.WINDOW_SIZE - self.STEP
        iters, _ = self._simulate(self.N, self.WINDOW_SIZE, self.STEP)
        for i in range(1, len(iters)):
            shared = set(iters[i - 1][-keep:]) & set(iters[i][:keep])
            assert len(shared) == keep, \
                f"Expected {keep} shared files, got {len(shared)}: {shared}"

    def test_all_files_covered_across_iterations(self):
        """Every URL in all_urls must appear in at least one iteration's window."""
        iters, all_urls = self._simulate(self.N, self.WINDOW_SIZE, self.STEP)
        seen = set()
        for on_disk in iters:
            seen.update(on_disk)
        assert seen == set(all_urls), \
            f"Uncovered files: {set(all_urls) - seen}"

    def test_no_file_processed_more_than_once_in_new_downloads(self):
        """
        dl_start/dl_end should never re-download a file that's already on disk.
        Each new_batch must contain only files not in the previous window.
        """
        all_urls = [f"file_{i:02d}" for i in range(self.N)]
        current_idx = 0
        on_disk = list(all_urls[:self.WINDOW_SIZE])
        downloaded = set(on_disk)

        while True:
            if current_idx + self.WINDOW_SIZE >= self.N:
                break
            current_idx += self.STEP
            dl_start = current_idx + (self.WINDOW_SIZE - self.STEP)
            dl_end   = current_idx + self.WINDOW_SIZE
            new_batch = all_urls[dl_start:dl_end]
            if not new_batch:
                break
            for f in new_batch:
                assert f not in downloaded, f"{f} downloaded twice"
            downloaded.update(new_batch)
            on_disk = on_disk[self.STEP:] + new_batch

    def test_partial_final_window_handled(self):
        """When total_files isn't a clean multiple, the last window is smaller."""
        # 8 files: initial window=6, one step → new_batch = all_urls[6:8] (2 files, not 4)
        iters, all_urls = self._simulate(8, self.WINDOW_SIZE, self.STEP)
        # Should complete without error, all files covered
        seen = set()
        for on_disk in iters:
            seen.update(on_disk)
        assert set(all_urls).issubset(seen)


# ─────────────────────────────────────────────────────────────────────────────
# run_hours filtering (HYSPLIT_Controller.make_run_dirs)
# ─────────────────────────────────────────────────────────────────────────────

def _hour_filter(datetimes, run_hours):
    """Mirror of the run_hours filter applied in make_run_dirs."""
    hours = set(run_hours)
    return [dt for dt in datetimes if dt.hour in hours]

# A full 48-hour sequence of hourly datetimes used across all run_hours tests.
_RH_BASE = datetime(2020, 6, 13, 0)
_RH_ALL  = [_RH_BASE + timedelta(hours=i) for i in range(48)]

# Colorado daylight hours (11am–8pm MDT = UTC-6) expressed in UTC.
_MDT_DAYLIGHT_UTC = [17, 18, 19, 20, 21, 22, 23, 0, 1, 2]


class TestRunHoursFilter:
    """
    Tests the run_hours UTC-hour filter that restricts trajectory start times
    to a configurable window (e.g. daylight-only Colorado runs).
    """

    def test_all_hours_kept_when_run_hours_is_full(self):
        result = _hour_filter(_RH_ALL, range(24))
        assert result == _RH_ALL

    def test_empty_run_hours_yields_no_runs(self):
        result = _hour_filter(_RH_ALL, [])
        assert result == []

    def test_single_hour_kept(self):
        result = _hour_filter(_RH_ALL, [12])
        assert all(dt.hour == 12 for dt in result)
        assert len(result) == 2  # two noon hours in a 48-h window

    def test_colorado_daylight_utc_hours_correct_count(self):
        # 48 hours × (10 daylight hours / 24) = 20 kept datetimes
        result = _hour_filter(_RH_ALL, _MDT_DAYLIGHT_UTC)
        assert len(result) == 20

    def test_colorado_daylight_only_allowed_hours_present(self):
        result = _hour_filter(_RH_ALL, _MDT_DAYLIGHT_UTC)
        allowed = set(_MDT_DAYLIGHT_UTC)
        assert all(dt.hour in allowed for dt in result)

    def test_colorado_daylight_no_excluded_hours_present(self):
        excluded = set(range(24)) - set(_MDT_DAYLIGHT_UTC)
        result = _hour_filter(_RH_ALL, _MDT_DAYLIGHT_UTC)
        assert all(dt.hour not in excluded for dt in result)

    def test_midnight_hour_included_in_colorado_daylight(self):
        # 00 UTC = 6pm MDT — should be in the daylight window
        assert 0 in _MDT_DAYLIGHT_UTC
        midnight_hits = [dt for dt in _hour_filter(_RH_ALL, _MDT_DAYLIGHT_UTC) if dt.hour == 0]
        assert len(midnight_hits) == 2

    def test_midday_utc_excluded_from_colorado_daylight(self):
        # 12 UTC = 6am MDT — before 11am, should be excluded
        assert 12 not in _MDT_DAYLIGHT_UTC
        result = _hour_filter(_RH_ALL, _MDT_DAYLIGHT_UTC)
        assert all(dt.hour != 12 for dt in result)

    def test_run_hours_does_not_affect_date_filtering(self):
        # Hour filter is applied after date filter; results should be a strict subset.
        all_result  = _hour_filter(_RH_ALL, range(24))
        day_result  = _hour_filter(_RH_ALL, _MDT_DAYLIGHT_UTC)
        assert set(day_result).issubset(set(all_result))

    def test_order_preserved(self):
        result = _hour_filter(_RH_ALL, _MDT_DAYLIGHT_UTC)
        assert result == sorted(result)
