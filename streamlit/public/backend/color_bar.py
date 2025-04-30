import public.backend.app_style_values as sv


def get_bar_color(score):
    if score >= 80:
        return sv.BAR_GREEN_COLOR
    if 60 <= score < 80:
        return sv.BAR_YELLOW_COLOR
    return sv.BAR_RED_COLOR

def get_icon(score):
    if score > 80:
        return "✅"
    if 60 <= score <= 80:
        return "⚠️"
    return "❌"

def custom_progress_bar(score, bar_color, icon):
    return f"""
        <div style="margin-bottom: 20px;">
            <div style="display: flex; align-items: center; margin-bottom: 5px;">
                <span style="font-size: 24px; margin-right: 10px;">{icon}</span>
                <span style="position: absolute; right: 0; font-weight: 600; font-size: 32px;">
                    {score:.2f}%
                </span>   
            </div>
            <div style="background-color: #ddd; height: 3px; border-radius: 4px; position: relative;">
                <div style="width: {score}%; background-color: {bar_color}; height: 100%; border-radius: 4px;"></div>
            </div>
        </div>
    """
