# this file is used to publish some procs that are used
# from the streamlit board
import re

import plotly.graph_objects as go
import streamlit as st

import public.backend.app_dependency_backend as backend

from public.backend.globals import *
from snowflake.snowpark.types import PandasDataFrame


def random_light_rgba_color():
   custom_colors = ["#61646B", "#A1E3A1", "#A1E3CA", "#A1C1E3", "#B5E2FF",
                    "#A1A8E3", "#C9A1E3", "#E3A1DB", "#EFACAC", "#EFC2A0", "#EFE3A0"]

   if not hasattr(random_light_rgba_color, "color_index"):
      random_light_rgba_color.color_index = 0
   color = custom_colors[random_light_rgba_color.color_index]
   random_light_rgba_color.color_index = (random_light_rgba_color.color_index + 1) % len(custom_colors)

   return color

@st.cache_data(show_spinner=True)
def generate_metadata(df_dependencies):
   labels = []
   sources =[]
   targets = []
   for FILE_ID,DEPENDENCIES,TOOL_EXECUTION_ID in df_dependencies.itertuples(index=False):
      if FILE_ID not in labels:
         labels.append(FILE_ID)
      if DEPENDENCIES is None:
         if " " not in labels:
            labels.append(" ")
         if labels.index(FILE_ID) not in sources and labels.index(FILE_ID) != labels.index(" "):
            targets.append(labels.index(" "))
            sources.append(labels.index(FILE_ID))
      else:
         DEPENDENCIES = re.subn(r'[\[\] \n"]', "", DEPENDENCIES)[0]
         if DEPENDENCIES == "" :
            if " " not in labels:
               labels.append(" ")
            if labels.index(FILE_ID) not in sources and labels.index(FILE_ID) != labels.index(" ") and labels.index(FILE_ID) not in targets:
               sources.append(labels.index(FILE_ID))
               targets.append(labels.index(" "))
         else:
            for dependency in DEPENDENCIES.split(","):

               if DEPENDENCIES not in labels:
                  labels.append(dependency)
               if labels.index(FILE_ID) !=labels.index(dependency):
                  sources.append(labels.index(FILE_ID))
                  targets.append(labels.index(dependency))
   return labels, sources, targets

@st.cache_data(show_spinner=True)
def filter_metadata(labels, sources, targets, selected_node):
   temp = []
   filtered_sources = []
   filtered_targets = []
   for source, target in zip(sources, targets, strict=False):
      if source == labels.index(selected_node):
         filtered_sources.append(source)
         filtered_targets.append(target)
   temp = filtered_targets
   new_targets = []
   for n in range(10):
      for source, target in zip(sources, targets, strict=False):
         if source in temp:
            filtered_sources.append(source)
            filtered_targets.append(target)
            new_targets.append(target)
      temp = new_targets
   return filtered_sources, filtered_targets

def dependency_analysis_single2(exec_id) -> (go.Figure, PandasDataFrame):
   df_dependencies = backend.get_unique_dependencies_by_execid(exec_id)
   labels, sources, targets = generate_metadata(df_dependencies)
   selected_node = st.selectbox("Select a file to filter:", set([labels[label] for label in sources]))
   filtered_sources, filtered_targets = filter_metadata(labels, sources, targets, selected_node)
   zipped_list = set(zip(filtered_sources, filtered_targets, strict=False))

   res = [[i for i, j in zipped_list],
           [j for i, j in zipped_list]]
   filtered_sources = res[0]
   filtered_targets = res[1]
   figure = generate_figure(labels, filtered_sources, filtered_targets)
   df_dependencies_by_file = generate_dependencies_by_file_table(selected_node, exec_id)
   return figure, df_dependencies_by_file

def generate_figure(labels, filtered_sources, filtered_targets):
   fig = go.Figure(data=[go.Sankey(
      textfont=dict(color="rgba(0,0,0,1)", size=12),
      arrangement="snap",
      node=dict(
         pad=5,
         thickness=30,
         line=dict(color="rgba(0, 0, 0, 0)", width=1),
         label=labels,
         hovertemplate=" ",
      ),
      link=dict(
         source=filtered_sources + [labels.index(" ")],
         target=filtered_targets + [labels.index(" ")],
         value=[1 for x in range(len(filtered_sources))] + [40],
         color=[random_light_rgba_color() if labels[x] != " " else "rgba(0, 0, 0, 0)" for x in
                filtered_targets + [labels.index(" ")]],
         hovertemplate=" ",
      ),
   )])

   fig.update_traces(hoverlabel_font_size=1, hoverlabel_font_color="rgba(0, 0, 0, 0)", selector=dict(type="sankey"))
   fig.update_traces(node_color=["rgba(0, 0, 0, 0)" if x == " " else "rgba(0, 0, 0, 255)" for x in labels])

   fig.update_layout(
      height=min(600, 50 * len(filtered_sources)),
      margin=dict(l=0, r=0, t=5, b=5),
   )
   return fig

def generate_dependencies_by_file_table(source_name: str, exec_id: str) -> PandasDataFrame:
   df_dependencies = backend.get_dependencies(exec_id)
   df_file_dependencies = df_dependencies[df_dependencies[FRIENDLY_NAME_SOURCE_FILE] == source_name]
   df_file_dependencies = df_file_dependencies[[FRIENDLY_NAME_SOURCE_FILE, COLUMN_IMPORT, COLUMN_DEPENDENCIES, COLUMN_PROJECT_ID, COLUMN_SUPPORTED, COLUMN_IS_BUILTIN]]
   df_file_dependencies[COLUMN_SUPPORTED] = df_file_dependencies[COLUMN_SUPPORTED].astype(str)
   df_file_dependencies[COLUMN_SUPPORTED] = df_file_dependencies[COLUMN_SUPPORTED].str.lower()
   df_file_dependencies[COLUMN_DEPENDENCIES] = df_file_dependencies[COLUMN_DEPENDENCIES].fillna("")
   df_file_dependencies[COLUMN_DEPENDENCIES] = df_file_dependencies[COLUMN_DEPENDENCIES].apply(lambda x: "" if x == "[]" else x)
   df_file_dependencies.columns = ["SOURCE FILE", "IMPORT", "INTERNAL DEPENDENCIES", "PROJECT ID", "AVAILABLE IN SNOWPARK", "BUILT-IN"]
   return df_file_dependencies

