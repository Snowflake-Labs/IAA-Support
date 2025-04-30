import pandas as pd

import public.backend.app_style_values as colors
import public.backend.app_style_values as style


colors = {
    "Not Supported": colors.LIGHT_RED_COLOR,
    "Workaround": colors.LIGTH_YELLOW_COLOR,
    "Direct Helper": colors.LIGHT_GREEN_COLOR,
    "Transformation": colors.LIGHT_GREEN_COLOR,
    "Direct": colors.LIGHT_GREEN_COLOR,
    "Rename": colors.LIGHT_GREEN_COLOR,
    "Helper": colors.LIGHT_GREEN_COLOR,
    "Rename Helper": colors.LIGHT_GREEN_COLOR,
}

def paint_mapping_status(value):
    if value is not None:
        mapping_status = value[0]
        if mapping_status is not None and mapping_status in colors:
            mapped_color =  style.getBackgroundColorProperty(colors[mapping_status])
            return  pd.Series({"Mapping Status": mapped_color})
