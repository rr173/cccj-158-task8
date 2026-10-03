"""DICOM 处理原语。

设计要点：
- 入库即冻结原始摘要（freeze_summary）：标签清单、像素参数、像素内嵌文字位置指纹，
  之后任何派生版本都不得修改该摘要。
- 标签清理（apply_tag_policy）按项目策略执行：删除/清空/替换为稳定伪名，
  同时清扫私有 group、曲线（50xx）、旧覆写层（60xx）中的身份残留。
- 像素遮盖（redact_burned_text）在像素层把内嵌患者文字区域涂黑；
  检测函数（detect_burned_text）同时充当质量检查门——检测不到残留才算合格。
"""
from __future__ import annotations

import datetime
import hashlib
import io

import numpy as np
import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid
from PIL import Image, ImageDraw

# 像素顶部注释带高度占比：演示影像中烧录的患者姓名/ID 位于该带内
BANNER_RATIO = 0.14
REDACTION_PAD = 6
DETECT_MIN_COLS = 8      # 笔画列阈值，低于此不认为存在文字


# ---------- 测试影像合成 ----------

def build_synthetic_dicom(*, patient_id: str = "MRN-48213",
                          patient_name: str = "ZHANG^WEI",
                          birth_date: str = "19790312",
                          accession: str = "ACC-77881",
                          study_desc: str = "CHEST XRAY",
                          institution: str = "CENTRAL HOSP",
                          burned_text: str | None = None,
                          rows: int = 256, cols: int = 256,
                          add_private: bool = True) -> bytes:
    """生成带烧录文字的合成 DICOM（8 位灰度），用于演示与测试。"""
    rng = np.random.default_rng(abs(hash((patient_id, accession))) % (2**32))
    pixels = rng.integers(20, 90, size=(rows, cols), dtype=np.uint8)
    # 中间画一个"解剖"结构，确保影像非纯噪声
    yy, xx = np.ogrid[:rows, :cols]
    disc = ((xx - cols / 2) ** 2 + (yy - rows * 0.55) ** 2) <= (min(rows, cols) * 0.28) ** 2
    pixels[disc] = 130

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.7"
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = Dataset()
    ds.preamble = b"\0" * 128
    ds.file_meta = file_meta
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    ds.PatientID = patient_id
    ds.PatientName = patient_name
    ds.PatientBirthDate = birth_date
    ds.PatientSex = "M"
    ds.AccessionNumber = accession
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.SOPClassUID = file_meta.MediaStorageSOPClassUID
    ds.Modality = "CR"
    ds.StudyDescription = study_desc
    ds.SeriesDescription = "PA VIEW"
    ds.InstitutionName = institution
    ds.StudyDate = "20260115"
    ds.StudyTime = "093012"
    ds.ContentDate = "20260115"
    ds.ContentTime = "093012"
    ds.PatientAge = "045Y"
    ds.ReferringPhysicianName = "DR^LI"
    ds.OperatorsName = "OP^CHEN"
    ds.BurnedInAnnotation = "YES"
    ds.Rows, ds.Columns = rows, cols
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"

    if add_private:
        block = ds.private_block(0x0041, "PHI_VENDOR", create=True)
        block.add_new(0x10, "LO", patient_name)  # 私有 PHI 残留

    text = burned_text if burned_text is not None else f"{patient_name}  {patient_id}"
    img = Image.fromarray(pixels)
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, cols, int(rows * BANNER_RATIO)], fill=18)
    draw.text((10, 6), text, fill=245)  # 顶部烧录身份文字
    pixels = np.array(img, dtype=np.uint8)
    ds.PixelData = pixels.tobytes()

    buf = io.BytesIO()
    ds.save_as(buf, write_like_original=False)
    return buf.getvalue()


# ---------- 冻结摘要 ----------

def freeze_summary(data: bytes) -> dict:
    """原始 DICOM 的不可变摘要：入库时计算一次，永久封存。"""
    ds = pydicom.dcmread(io.BytesIO(data), force=True)
    identity = {
        "PatientID": str(ds.get("PatientID", "")),
        "PatientName": str(ds.get("PatientName", "")),
        "PatientBirthDate": str(ds.get("PatientBirthDate", "")),
        "PatientSex": str(ds.get("PatientSex", "")),
        "AccessionNumber": str(ds.get("AccessionNumber", "")),
    }
    elements = sorted(f"{e.tag.group:04X},{e.tag.element:04X}:{e.keyword or 'Private'}"
                      for e in ds.iterall() if e.tag != 0x7FE00010)
    pixels = ds.pixel_array
    banner = pixels[: max(1, int(pixels.shape[0] * BANNER_RATIO))]
    return {
        "frozen_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "sop_uid": str(ds.get("SOPInstanceUID", "")),
        "study_uid": str(ds.get("StudyInstanceUID", "")),
        "modality": str(ds.get("Modality", "")),
        "identity": identity,
        "elements": elements,
        "pixel_shape": list(pixels.shape),
        "burned_in_annotation_flag": str(ds.get("BurnedInAnnotation", "")),
        "banner_text_detected": detect_burned_text(pixels)["present"],
        "pixel_sha256": hashlib.sha256(pixels.tobytes()).hexdigest(),
    }


def extract_identity(data: bytes) -> dict:
    return freeze_summary(data)["identity"]


# ---------- 标签策略 ----------

def default_policy(level: str = "strict") -> dict:
    """项目去标识化策略。策略只增版本、永不就地修改。"""
    delete_tags = ["PatientBirthDate", "PatientSex", "PatientAge", "AccessionNumber",
                   "ReferringPhysicianName", "OperatorsName", "InstitutionName",
                   "StudyDate", "StudyTime", "ContentDate", "ContentTime",
                   "StudyDescription", "SeriesDescription"]
    policy = {
        "policy_version": 1,
        "level": level,
        "delete_tags": delete_tags,
        "replace_tags": ["PatientID", "PatientName"],   # 替换为稳定伪名
        "remove_private_groups": True,
        "remove_overlays_and_curves": True,
        "redact_burned_text": True,
        "require_burned_text_clear": True,
    }
    if level == "minimal":
        policy["delete_tags"] = ["PatientBirthDate", "AccessionNumber",
                                 "ReferringPhysicianName", "OperatorsName"]
    return policy


def apply_tag_policy(data: bytes, policy: dict, pseudo_id: str, pseudo_name: str) -> tuple[bytes, list]:
    ds = pydicom.dcmread(io.BytesIO(data), force=True)
    steps: list[dict] = []
    removed, replaced = [], []

    for tag in policy.get("delete_tags", []):
        if tag in ds:
            del ds[tag]
            removed.append(tag)
    steps.append({"step": "tag_delete", "detail": {"tags": removed}})

    if "PatientID" in policy.get("replace_tags", []):
        if "PatientID" in ds:
            ds.PatientID = pseudo_id
            replaced.append("PatientID")
    if "PatientName" in policy.get("replace_tags", []):
        ds.PatientName = pseudo_name
        replaced.append("PatientName")
    steps.append({"step": "tag_replace",
                  "detail": {"tags": replaced, "phi_values_logged": False}})

    if policy.get("remove_private_groups", True):
        priv = [e.keyword or f"{e.tag}" for e in ds if e.tag.group % 2 == 1 and not e.tag.is_private_creator]
        creators = [e.tag for e in ds if e.tag.is_private_creator]
        ds.remove_private_tags()
        steps.append({"step": "private_tags_purged",
                      "detail": {"private_elements": len(priv), "creators": len(creators)}})

    if policy.get("remove_overlays_and_curves", True):
        n_overlay = n_curve = 0
        for e in list(ds):
            if 0x6000 <= e.tag.group <= 0x60FF and (e.tag.group & 0x1) == 0:
                del ds[e.tag]; n_overlay += 1
            if 0x5000 <= e.tag.group <= 0x50FF:
                del ds[e.tag]; n_curve += 1
        steps.append({"step": "overlays_curves_purged",
                      "detail": {"overlays": n_overlay, "curves": n_curve}})

    # 元数据自证
    ds.BurnedInAnnotation = "NO"
    ds.DeidentificationMethod = "MEDDEID_PIPELINE"
    out = io.BytesIO()
    ds.save_as(out, write_like_original=False)
    return out.getvalue(), steps


# ---------- 像素文字检测与遮盖 ----------

def _banner(pixels: np.ndarray) -> np.ndarray:
    return pixels[: max(1, int(pixels.shape[0] * BANNER_RATIO))]


def detect_burned_text(pixels: np.ndarray) -> dict:
    """检测顶部注释带内的亮文字。

    判据：带内高亮（>=150）笔画列总数。合成/真实背景与解剖结构均低于该亮度，
    只有烧录的白色注释文字会形成成批亮列；遮盖后整带归零即判为清除。
    """
    banner = _banner(pixels)
    bright = banner >= 150
    col_hits = bright.sum(axis=0)
    active = col_hits >= 3  # 该列至少 3 行亮像素才算笔画列
    stroke_cols = int(active.sum())
    return {"present": stroke_cols >= DETECT_MIN_COLS, "stroke_cols": stroke_cols,
            "banner_h": int(banner.shape[0])}


def redact_burned_text(data: bytes) -> tuple[bytes, dict]:
    ds = pydicom.dcmread(io.BytesIO(data), force=True)
    pixels = ds.pixel_array.copy()
    det = detect_burned_text(pixels)
    detail = {"detected_before": det["present"], "region": None}
    if det["present"]:
        h = det["banner_h"]
        banner = pixels[:h]
        active = (banner >= 150).sum(axis=0) >= 3
        idxs = np.where(active)[0]
        x0 = max(0, int(idxs[0]) - REDACTION_PAD)
        x1 = min(pixels.shape[1], int(idxs[-1]) + 1 + REDACTION_PAD)
        pixels[0:h, x0:x1] = 0
        detail["region"] = {"x0": x0, "x1": x1, "y0": 0, "y1": h}
    after = detect_burned_text(pixels)
    detail["detected_after"] = after["present"]
    ds.PixelData = pixels.tobytes()
    out = io.BytesIO()
    ds.save_as(out, write_like_original=False)
    return out.getvalue(), detail


def render_preview_png(data: bytes) -> bytes:
    ds = pydicom.dcmread(io.BytesIO(data), force=True)
    arr = ds.pixel_array
    lo, hi = np.percentile(arr, (2, 98))
    norm = np.clip((arr - lo) / max(hi - lo, 1), 0, 255).astype(np.uint8)
    buf = io.BytesIO()
    Image.fromarray(norm).resize((256, 256)).convert("RGB").save(buf, format="PNG")
    return buf.getvalue()


# ---------- 质量检查 ----------

PHI_KEYWORDS = ("PatientBirthDate", "PatientSex", "PatientAge",
                "AccessionNumber", "ReferringPhysicianName", "OperatorsName",
                "InstitutionName", "StudyDate", "StudyTime",
                "ContentDate", "ContentTime")


def quality_check(data: bytes, policy: dict,
                  expect_pseudo_id: str | None = None,
                  expect_pseudo_name: str | None = None) -> dict:
    """派生版本发布前的质量门：标签 PHI、私有标签、像素残留三类检查全部通过才放行。"""
    ds = pydicom.dcmread(io.BytesIO(data), force=True)
    problems: list[str] = []
    checks: dict = {}

    # 策略声明应删除的身份标签若仍存在即泄漏；replace 标签（PatientID/Name）
    # 必须恰好等于替代身份，单独在 pseudo_identity 检查中校验
    required_deleted = tuple(
        kw for kw in PHI_KEYWORDS if kw in policy.get("delete_tags", []))
    leaked = [kw for kw in required_deleted
              if kw in ds and str(ds[kw].value).strip() not in ("", "N/A")]
    if leaked:
        problems.append(f"phi_tags_present:{leaked}")
    checks["phi_tags"] = {"pass": not leaked, "leaked": leaked}

    priv = [e for e in ds if e.tag.group % 2 == 1]
    if policy.get("remove_private_groups", True) and priv:
        problems.append(f"private_tags_remaining:{len(priv)}")
    checks["private_tags"] = {"pass": not priv, "count": len(priv)}

    det = detect_burned_text(ds.pixel_array)
    if policy.get("require_burned_text_clear", True) and det["present"]:
        problems.append("burned_text_still_visible")
    checks["burned_text"] = {"pass": not det["present"], **det}

    pseudo_id_ok = "PatientID" in ds and (
        expect_pseudo_id is None or str(ds.PatientID) == expect_pseudo_id)
    pseudo_name_ok = "PatientName" in ds and (
        expect_pseudo_name is None or str(ds.PatientName) == expect_pseudo_name)
    if not (pseudo_id_ok and pseudo_name_ok):
        problems.append("pseudo_identity_incorrect")
    checks["pseudo_identity"] = {
        "pass": pseudo_id_ok and pseudo_name_ok,
        "patient_id": str(ds.get("PatientID", "")),
        "patient_name": str(ds.get("PatientName", ""))}
    return {"pass": not problems, "problems": problems, "checks": checks}
