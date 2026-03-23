def classify_event(payload):
    speed = payload.get("speed_kmh") or 0
    ax = payload.get("accel_x") or 0
    ay = payload.get("accel_y") or 0

    if ax < -2.5 or ay < -2.5:
        return "harsh_braking", 0.9
    if speed > 35:
        return "speeding", 0.7
    return "normal", 0.1