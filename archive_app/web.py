from __future__ import annotations

import csv
import io
import os
import threading

from flask import (
    Flask,
    Response,
    abort,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)
from werkzeug.exceptions import RequestEntityTooLarge

from .catalog import (
    MAX_SPAN_DAYS,
    STATUS_BLOCKED,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_PENDING,
    Store,
    parse_day,
    source_from_env,
)
from .collector import collect
from .csv_import import (
    CsvValidationError,
    parse_correction_csv,
    parse_csv,
    parse_manifest,
    verify_correction_batch,
    verify_includes_collected,
)

STATUS_LABELS = {
    STATUS_PENDING: "待查询",
    STATUS_COMPLETE: "已查全",
    STATUS_BLOCKED: "待补档",
    STATUS_FAILED: "待重试",
}


def create_app(
    database: str | None = None,
    source=None,
    testing: bool = False,
) -> Flask:
    app = Flask(__name__)
    app.config.update(
        SECRET_KEY=os.environ.get("SECRET_KEY", "dev-only-change-me"),
        DATABASE=database or os.environ.get("DATABASE", "/data/archive.db"),
        MAX_CONTENT_LENGTH=4 * 1024 * 1024,
        TESTING=testing,
    )
    store = Store(app.config["DATABASE"])
    store.init()
    app.extensions["store"] = store
    app.extensions["source"] = source or source_from_env()
    work_lock = threading.Lock()

    def current_store() -> Store:
        return app.extensions["store"]

    def current_source():
        return app.extensions["source"]

    @app.template_filter("status_label")
    def status_label(value: str) -> str:
        return STATUS_LABELS.get(value, value)

    @app.get("/")
    def index():
        st: Store = current_store()
        return render_template(
            "index.html",
            runs=st.runs(),
            max_span=MAX_SPAN_DAYS,
            page_size=current_source().page_size(),
        )

    @app.post("/runs")
    def create_run():
        st: Store = current_store()
        try:
            start = parse_day(request.form.get("start_date", ""))
            end = parse_day(request.form.get("end_date", ""))
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))
        if end < start:
            flash("结束日期不能早于开始日期", "error")
            return redirect(url_for("index"))
        span = (end - start).days + 1
        if span > MAX_SPAN_DAYS:
            flash(f"一次最多选择 {MAX_SPAN_DAYS} 个连续日", "error")
            return redirect(url_for("index"))
        run_id = st.create_run(start, end)
        flash(f"已创建 {start} 至 {end} 的归档任务", "info")
        return redirect(url_for("run_detail", run_id=run_id))

    @app.get("/runs/<int:run_id>")
    def run_detail(run_id: int):
        st: Store = current_store()
        run = st.get_run(run_id)
        if run is None:
            abort(404)
        days = st.days(run_id)
        counts = {status: 0 for status in STATUS_LABELS}
        for row in days:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        manifest_meta = st.manifest_meta(run_id)
        diff = st.diff(run_id)
        return render_template(
            "run.html",
            run=run,
            days=days,
            counts=counts,
            labels=STATUS_LABELS,
            ready=st.is_ready(run_id),
            downloadable=st.is_downloadable(run_id),
            manifest_meta=manifest_meta,
            diff=diff,
            events=st.events(run_id),
        )

    def _read_upload(field: str):
        upload = request.files.get(field)
        if upload is None or not upload.filename:
            return None
        return upload

    @app.post("/runs/<int:run_id>/manifest")
    def register_manifest(run_id: int):
        st: Store = current_store()
        run = st.get_run(run_id)
        if run is None:
            abort(404)
        upload = _read_upload("csv")
        if upload is None:
            flash("请选择清单 CSV 文件", "error")
            return redirect(url_for("run_detail", run_id=run_id))
        data = upload.read()
        try:
            manifest = parse_manifest(data, run["start_date"], run["end_date"])
            text = data.decode("utf-8-sig", errors="replace")
            st.set_manifest(run_id, text, manifest, upload.filename)
        except CsvValidationError as exc:
            flash("清单校验失败，未做任何变更：" + "；".join(exc.errors), "error")
            return redirect(url_for("run_detail", run_id=run_id))
        except ValueError as exc:
            flash(f"清单登记失败，未做任何变更：{exc}", "error")
            return redirect(url_for("run_detail", run_id=run_id))

        diff = st.diff(run_id)
        if diff and diff.has_differences:
            flash(
                f"已登记清单（{len(manifest)} 条）。存在差异，目录下载已暂停，"
                "请勾选有差异的日期并上传修正后的完整事件 CSV。",
                "warn",
            )
        else:
            flash(f"已登记清单（{len(manifest)} 条），与归档完全一致。", "info")
        return redirect(url_for("run_detail", run_id=run_id))

    @app.post("/runs/<int:run_id>/corrections")
    def apply_corrections(run_id: int):
        st: Store = current_store()
        run = st.get_run(run_id)
        if run is None:
            abort(404)
        manifest = st.manifest(run_id)
        if manifest is None:
            flash("请先登记事件清单", "warn")
            return redirect(url_for("run_detail", run_id=run_id))

        selected = [
            value
            for value in request.form.getlist("days")
            if any(row["day"] == value for row in st.days(run_id))
        ]
        if not selected:
            flash("请勾选要修正的日期", "error")
            return redirect(url_for("run_detail", run_id=run_id))

        upload = _read_upload("csv")
        if upload is None:
            flash("请选择修正后的完整事件 CSV 文件", "error")
            return redirect(url_for("run_detail", run_id=run_id))

        collected = st.collected_by_day(run_id)
        try:
            parsed = parse_correction_csv(upload.read(), selected, manifest, collected)
            diff = st.diff(run_id)
            if diff is None:  # pragma: no cover - checked above
                raise ValueError("清单尚未登记")
            verify_correction_batch(selected, diff)
            st.apply_corrections(run_id, parsed)
        except CsvValidationError as exc:
            flash("修正文件校验失败，归档保持不变：" + "；".join(exc.errors), "error")
            return redirect(url_for("run_detail", run_id=run_id))
        except KeyError:
            flash("所选日期不在任务内，归档保持不变", "error")
            return redirect(url_for("run_detail", run_id=run_id))
        except ValueError as exc:
            flash(f"修正失败，归档保持不变：{exc}", "error")
            return redirect(url_for("run_detail", run_id=run_id))

        remaining = st.diff(run_id)
        if remaining and remaining.has_differences:
            flash(
                f"已修正 {len(selected)} 个日期，仍有差异，下载继续暂停。",
                "warn",
            )
        else:
            flash(f"已修正 {len(selected)} 个日期，清单与归档完全一致。", "info")
        return redirect(url_for("run_detail", run_id=run_id))

    @app.get("/runs/<int:run_id>/manifest.csv")
    def download_manifest(run_id: int):
        st: Store = current_store()
        if st.get_run(run_id) is None:
            abort(404)
        manifest = st.manifest(run_id)
        if manifest is None:
            abort(404)
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["event_id", "day"])
        for event_id, day in sorted(manifest.items(), key=lambda item: (item[1], item[0])):
            writer.writerow([event_id, day])
        return Response(
            buffer.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="manifest_run{run_id}.csv"',
                "Cache-Control": "no-store",
            },
        )

    @app.post("/runs/<int:run_id>/collect")
    def run_collect(run_id: int):
        st: Store = current_store()
        if st.get_run(run_id) is None:
            abort(404)
        if not work_lock.acquire(blocking=False):
            flash("已有采集或补档操作在进行，请稍后重试", "warn")
            return redirect(url_for("run_detail", run_id=run_id))
        try:
            summary = collect(st, current_source(), run_id)
            flash(
                f"本次采集：查全 {summary.completed} 天，待补档 {summary.blocked} 天，"
                f"待重试 {summary.failed} 天",
                "info" if not summary.failed else "warn",
            )
        finally:
            work_lock.release()
        return redirect(url_for("run_detail", run_id=run_id))

    @app.post("/runs/<int:run_id>/days/<day>/upload")
    def upload_day(run_id: int, day: str):
        st: Store = current_store()
        run = st.get_run(run_id)
        if run is None:
            abort(404)
        rows = [r for r in st.days(run_id) if r["day"] == day]
        if not rows:
            abort(404)
        if rows[0]["status"] != STATUS_BLOCKED:
            flash(f"{day} 不是待补档日期，无需上传", "warn")
            return redirect(url_for("run_detail", run_id=run_id))

        upload = request.files.get("csv")
        if upload is None or not upload.filename:
            flash("请选择 CSV 文件", "error")
            return redirect(url_for("run_detail", run_id=run_id))

        try:
            parsed = parse_csv(upload.read(), day)
            st.backfill_day(
                run_id,
                day,
                parsed.events,
                verify=lambda existing: verify_includes_collected(parsed, existing),
            )
        except CsvValidationError as exc:
            flash("补档文件校验失败，进度保持不变：" + "；".join(exc.errors), "error")
            return redirect(url_for("run_detail", run_id=run_id))
        except ValueError as exc:
            flash(f"补档失败，进度保持不变：{exc}", "error")
            return redirect(url_for("run_detail", run_id=run_id))

        flash(f"{day} 已补档，共 {len(parsed.events)} 条", "info")
        return redirect(url_for("run_detail", run_id=run_id))

    @app.get("/runs/<int:run_id>/download")
    def download_run(run_id: int):
        st: Store = current_store()
        run, allowed, events = st.snapshot_for_download(run_id)
        if run is None:
            abort(404)
        if not allowed:
            flash("还有未查全、未补档或与清单不一致的日期，暂不能下载完整目录", "error")
            return redirect(url_for("run_detail", run_id=run_id))
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["event_id", "utc_time", "magnitude"])
        for row in events:
            writer.writerow([row["event_id"], row["utc_time"], f"{row['magnitude']:g}"])
        filename = f"catalog_{run['start_date']}_{run['end_date']}.csv"
        return Response(
            buffer.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Cache-Control": "no-store",
            },
        )

    @app.errorhandler(404)
    def not_found(_exc):
        return render_template("404.html"), 404

    @app.errorhandler(RequestEntityTooLarge)
    def too_large(_exc):
        flash("补档文件过大（上限 4 MB）", "error")
        return redirect(request.referrer or url_for("index"))

    return app
