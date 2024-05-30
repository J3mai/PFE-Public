import streamlit as st
import pandas as pd
import warnings
from sklearn.ensemble import RandomForestRegressor
import joblib

warnings.filterwarnings("ignore")
cities = [
    "Albuquerque", "Allston", "Alviso", "Anaheim", "Anchorage", "Antioch", "Arlington",
    "Asheville", "Astoria", "Atlanta", "Augusta", "Aurora", "Austin", "Bakersfield", 
    "Bala Cynwyd", "Baltimore", "Baton Rouge", "Bay View", "Bayside", "Berkeley", 
    "Blacklick", "Boca Raton", "Boise", "Boston", "Boynton Beach", "Brentwood", 
    "Briarwood", "Brighton", "Bronx", "Brooklyn", "Brooklyn Heights", "Buffalo", 
    "Camby", "Canal Winchester", "Canoga Park", "Canton", "Carnegie", "Carson", 
    "Chapel Hill", "Charleston", "Charlestown", "Charlotte", "Chattanooga", 
    "Cheektowaga", "Chicago", "Cincinnati", "Cleveland", "College Point", 
    "Colorado Springs", "Columbus", "Coral Gables", "Cordova", "Corona", "Crowley", 
    "Cupertino", "Dallas", "Daly City", "Daniel Island", "Del Mar", "Delray Beach", 
    "Denver", "Des Moines", "Detroit", "Dorchester", "Dublin", "Durham", "Eagle River", 
    "East Boston", "El Paso", "Elk Grove", "Elkins Park", "Elmhurst", "Emeryville", 
    "Encino", "Englewood", "Evanston", "Fairdale", "Fairlawn", "Far Rockaway", 
    "Florissant", "Flushing", "Forest Hills", "Fort Lauderdale", "Fort Worth", 
    "Fresh Meadows", "Fresno", "Friendswood", "Ft Lauderdale", "Ft Worth", "Galloway", 
    "Gardena", "Gilbert", "Gladstone", "Glendale", "Grandview", "Greenacres", 
    "Greensboro", "Grove City", "Gwynn Oak", "Harrison", "Henderson", "Hephzibah", 
    "Highland Park", "Hilliard", "Hixson", "Hollis", "Hollywood", "Honolulu", "Houston", 
    "Hyde Park", "Indianapolis", "Irving", "Jackson Heights", "Jacksonville", "Jamaica", 
    "Jamaica Plain", "Justin", "Kansas City", "Kew Gardens", "Kingwood", "Knoxville", 
    "La Jolla", "Lake Balboa", "Lake Oswego", "Las Vegas", "Lewis Center", "Lexington", 
    "Lincoln", "Little Neck", "Littleton", "Long Beach", "Long Island City", "Los Angeles", 
    "Louisville", "Macomb", "Madison", "Marina Del Rey", "Maspeth", "Mattapan", "Memphis", 
    "Mesa", "Miami", "Miami Beach", "Middle Village", "Milwaukee", "Minneapolis", 
    "Montgomery", "Mt Washington", "Nashville", "New Albany", "New Haven", "New Orleans", 
    "New York", "Newark", "Norfolk", "Norman", "North Hills", "North Hollywood", 
    "North Las Vegas", "Northridge", "Oakland", "Oakland Gardens", "Oklahoma City", 
    "Old Hickory", "Omaha", "Ooltewah", "Orlando", "Ozone Park", "Pacific Palisades", 
    "Parkville", "Penn Hills", "Phila", "Philadelphia", "Phoenix", "Pikesville", 
    "Pittsburgh", "Plano", "Playa Del Rey", "Portland", "Providence", "Queens", 
    "Queens Village", "Raleigh", "Ralston", "Rancho Cascades", "Rego Park", "Reno", 
    "Reseda", "Reynoldsburg", "Richmond", "Ridgewood", "Rochester", "Rosedale", 
    "Roslindale", "Roxbury", "Roxbury Crossing", "Sacramento", "Saint Louis", 
    "Saint Petersburg", "Salt Lake City", "Salt Lake Cty", "San Antonio", "San Diego", 
    "San Francisco", "San Jose", "San Pedro", "San Ysidro", "Sanford", "Santa Ana", 
    "Santa Monica", "Scottsdale", "Seattle", "Shaker Heights", "Sherman Oaks", 
    "Shreveport", "Slc", "South Boston", "South Salt Lake", "Spokane", "Springfield Gardens", 
    "Squirrel Hill", "St Petersburg", "St. Louis", "St. Petersburg", "Staten Island", 
    "Studio City", "Sun Valley", "Sunnyside", "Syracuse", "Tallahassee", "Tampa", 
    "Tarzana", "The Bronx", "Toluca Lake", "Torrance", "Tucson", "Tujunga", "Tulsa", 
    "Urban Honolulu", "Va Beach", "Valley Glen", "Valley Village", "Van Nuys", "Venice", 
    "Virginia Beach", "Von Ormy", "Washington", "Webster", "West Hollywood", 
    "West Roxbury", "Westerville", "Weston", "Whitestone", "Wichita", "Wilmington", 
    "Winnetka", "Winston Salem", "Winston-Salem", "Winter Springs", "Woodland Hills", 
    "Woodside", "Worthington", "Yukon"
]

model = joblib.load('models/CatBOOST_model.joblib')
#streamlit
st.title("Rental House Price Prediction App")
input_method = st.sidebar.radio("Select Input Method", ("Manual Entry", "Upload CSV"))
prediction = 0
if input_method == "Manual Entry":
    # Create input fields for user input
    Beds = st.number_input("Beds", 0, 10, 0)
    Baths = st.number_input("Baths", 0, 10, 0)
    Surface = st.number_input("Surface", 0, 20000, 0)
    City = st.selectbox (
        label= "Select the city",
        options=cities
        )
    # Make predictions
    input_data = pd.DataFrame(
        {
            "Beds": [Beds],
            "Baths": [Baths],
            "Surface": [Surface],
        }
    )
    for c in cities:
        if c == City :
            input_data[c] = 1
        else:
            input_data[c] = 0
    
    prediction = model.predict(input_data)


elif input_method == "Upload CSV":
    uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

    if uploaded_file is not None:
        # Read the uploaded CSV file
        input_data = pd.read_csv(uploaded_file)

        # Show the uploaded data
        st.subheader("Uploaded Data:")
        st.write(input_data)

        # Make predictions
        prediction = model.predict(input_data)

st.subheader("Prediction:")
if prediction :
    st.write(f"The house monthly rent is approximately : {prediction[0]:.2f} USD")
    
else:
    st.write("DATA NOT FOUND FOR PREDICTION")

# st.sidebar.subheader("Model Metrics:")
# with st.sidebar:
#     st.write("Accuracy Score:", best_run["metric.acurracy"])
#     st.write("Precision Score:", best_run["metric.precision_score"])
#     st.write("Recall Score:", best_run["metric.recall_score"])
#     st.write("F1 Score:", best_run["metric.f1_score"])