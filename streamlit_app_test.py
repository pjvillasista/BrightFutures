import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="BrightFutures",
    page_icon="🧑‍🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)


# Function to load and preprocess data
@st.cache_data
def load_data():
    df = pd.read_csv("data/staging/stg_all_schools.csv")
    df = df.dropna(subset=["lat", "lon"])  # Ensure lat and lon are not NaN
    # Ensure data types are correct, e.g., boolean columns for filters
    boolean_columns = [
        "IsPreK",
        "IsElementary",
        "IsMiddle",
        "IsHigh",
        "Private",
        "Public District",
        "Public Charter",
    ]
    for col in boolean_columns:
        df[col] = df[col].astype(bool)
    return df


df = load_data()

# Sidebar Filters
with st.sidebar:
    st.title("🧑‍🎓 BrightFutures")
    school_query = st.text_input("Search for a school", "")

    selected_cities = st.multiselect(
        "Select cities", ["All"] + sorted(df["City"].unique()), ["All"]
    )
    grade_levels = ["Pre-K", "Elementary", "Middle", "High"]
    selected_grades = st.multiselect(
        "Select grade levels", ["All"] + grade_levels, ["All"]
    )
    school_type_options = ["Private", "Public District", "Public Charter"]
    selected_school_types = st.multiselect(
        "Select school types", ["All"] + school_type_options, ["All"]
    )
    score_categories = [
        "All",
        "Below Average",
        "Average",
        "Above Average",
    ]
    selected_score_category = st.selectbox("Select score category", score_categories)
    include_data_not_available = st.toggle("Include Schools with no data", value=True)


def filter_data(df):
    # Filter by school query
    if school_query:
        df = df[df["School Name"].str.contains(school_query, case=False)]

    # Filter by selected cities
    if "All" not in selected_cities:
        df = df[df["City"].isin(selected_cities)]

    # Filter by score category
    if selected_score_category != "All":
        df = df[df["Score Category"] == selected_score_category]

    # Grade level filter
    grade_filters = []
    if "All" not in selected_grades:
        if "Pre-K" in selected_grades:
            grade_filters.append(df["IsPreK"])
        if "Elementary" in selected_grades:
            grade_filters.append(df["IsElementary"])
        if "Middle" in selected_grades:
            grade_filters.append(df["IsMiddle"])
        if "High" in selected_grades:
            grade_filters.append(df["IsHigh"])
        if grade_filters:
            combined_grade_filter = pd.concat(grade_filters, axis=1).any(axis=1)
            df = df[combined_grade_filter]

    # School type filter
    type_filters = []
    if "All" not in selected_school_types:
        if "Private" in selected_school_types:
            type_filters.append(df["Private"])
        if "Public District" in selected_school_types:
            type_filters.append(df["Public District"])
        if "Public Charter" in selected_school_types:
            type_filters.append(df["Public Charter"])
        if type_filters:
            combined_type_filter = pd.concat(type_filters, axis=1).any(axis=1)
            df = df[combined_type_filter]

    global include_data_not_available  # Ensure access to the variable
    # Exclude "Data Not Available" if the checkbox is unchecked
    if not include_data_not_available:
        df = df[df["Score Category"] != "Data Not Available"]

    return df


filtered_df = filter_data(df)

st.markdown("# BrightFutures")
st.markdown("## This dashboard provides education statistics across SoCal cities🏙️")

with st.expander("KPIs", expanded=True):
    st.title("KPIs 📊")
    title_style = "style='font-size: 1.25em; font-weight: bold;'"
    large_number_style = "style='font-size: 2.5em; display: inline;'"
    smaller_percentage_style = "style='font-size: 1.25em; display: inline;'"

    average_composite_score = filtered_df["Composite Score"].mean().round()
    schools_by_category = filtered_df["Score Category"].value_counts()
    top_performing_schools = len(
        filtered_df[
            filtered_df["Composite Score"] == filtered_df["Composite Score"].max()
        ]
    )
    schools_needing_attention = schools_by_category.get("Below Average", 0)

    # Calculate the total number of schools with a score category assigned (excluding 'Data Not Available' if included)
    total_schools_with_scores = len(
        filtered_df[filtered_df["Score Category"] != "Data Not Available"]
    )

    # Calculate the percentage of schools needing attention
    if total_schools_with_scores > 0:
        percentage_needing_attention = (
            schools_needing_attention / total_schools_with_scores
        ) * 100
    else:
        percentage_needing_attention = 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(
            f"""
        <div {title_style}>Average Composite Score</div>
        <span {large_number_style}>{average_composite_score:.2f}</span>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
        <div {title_style}>Top Performing Schools</div>
        <span {large_number_style}>{top_performing_schools}</span>
        """,
            unsafe_allow_html=True,
            help="Indicates the number of schools achieving the highest possible composite score, showcasing the top performers. These institutions set the standard for excellence and are models of high achievement based on the evaluation criteria.",
        )

    with col3:
        st.markdown(
            f"""
        <div {title_style}>🚨Schools Needing Attention</div>
        <span {large_number_style}>{schools_needing_attention}</span>
        <span {smaller_percentage_style}>({percentage_needing_attention:.2f}%)</span>
        """,
            unsafe_allow_html=True,
            help="This number represents schools classified as 'Below Average' based on their Composite Score. It highlights institutions that might need additional resources, interventions, or policy changes to improve their educational outcomes.",
        )

    # Calculate the number and percentage of 'Above Average' schools for column 4
    above_average_schools = schools_by_category.get("Above Average", 0)
    percentage_above_average = (
        (above_average_schools / total_schools_with_scores) * 100
        if total_schools_with_scores > 0
        else 0
    )

    with col4:
        st.markdown(
            f"""
        <div {title_style}>Above Average Schools</div>
        <span {large_number_style}>{above_average_schools}</span>
        <span {smaller_percentage_style}>({percentage_above_average:.2f}%)</span>
        """,
            unsafe_allow_html=True,
        )

tab1, tab2, tab3 = st.tabs(
    ["📈 Charts & Data", "Sentiment", "How Scores are Calculated?"]
)

with tab1:
    st.markdown("## Map of Schools 🗺️")
    with st.expander("Interactive School Performance Map⬇️", expanded=True):
        st.markdown("### Interactive School Performance Map")

        default_size = 0.7  # Choose a reasonable default size for your map
        filtered_df = filtered_df.copy()
        filtered_df["Visual Size"] = filtered_df["Composite Score"].fillna(default_size)

        fig = px.scatter_mapbox(
            filtered_df,
            lat="lat",
            lon="lon",
            color="Score Category",
            size="Visual Size",
            size_max=12,
            opacity=0.75,
            hover_name="School Name",
            hover_data={
                "Address": True,
                "Composite Score": True,
                "Score Category": False,
                "Academic Progress": True,
                "Test Scores": True,
                "Star Rating": False,
                "Visual Size": False,
                "lat": False,
                "lon": False,
                "School Types": True,
            },
            color_discrete_map={
                "Below Average": "crimson",
                "Average": "orange",
                "Above Average": "lightskyblue",
                "Data Not Available": "grey",
            },
            zoom=10,
            center=go.layout.mapbox.Center(lat=34.0522, lon=-118.2437),
        )
        fig.update_layout(mapbox_style="carto-darkmatter")
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=800)
        fig.update_traces(hoverlabel=dict(namelength=-1, font_size=16))
        # Display the map
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("# Data🖥️")
    with st.expander("School Details", expanded=True):
        # Display the filtered dataframe
        st.dataframe(
            filtered_df[
                [
                    "School Name",
                    "City",
                    "Address",
                    "Academic Progress",
                    "Test Scores",
                    "Star Rating",
                    "Score Category",
                    "Composite Score",
                    "lat",
                    "lon",
                ]
            ],
            use_container_width=True,
        )

# Inject custom CSS
css = """
<style>
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 1.20rem; 
    }
</style>
"""

st.markdown(css, unsafe_allow_html=True)
