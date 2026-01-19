from .normalize import canon_row, row_hash

def compute_diff(new_rows: list[dict], baseline_by_din: dict[str, dict]):
    new_by_din = {r["DIN"]: canon_row(r) for r in new_rows if r.get("DIN")}
    added, removed, modified = [], [], []

    for din, nr in new_by_din.items():
        br = baseline_by_din.get(din)
        if br is None:
            added.append({"din": din, "after": nr})
        elif row_hash(nr) != row_hash(br):
            modified.append({"din": din, "before": br, "after": nr})

    for din, br in baseline_by_din.items():
        if din not in new_by_din:
            removed.append({"din": din, "before": br})

    return added, removed, modified
