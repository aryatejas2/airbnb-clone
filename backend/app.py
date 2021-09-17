from flask import Flask, request, jsonify
import pymongo
from pymongo import MongoClient
import csv
# from flask_cors import CORS

app = Flask(__name__)
# cors=CORS(app)

def get_db():
    client = MongoClient(host='airbnb_mongodb',
                         port=27017, 
                         username='root', 
                         password='pass',
                        authSource="admin")
    db = client['airbnb_db']
    return db

@app.route('/')
def ping_server():
    response = jsonify("Server is working.")
    response.headers.add("Access-Control-Allow-Origin","*")
    return response

@app.route('/count')
def count():
    db=""
    try:
        db = get_db()
        col = db["airbnb_hostings"]
        count = col.count()
        return jsonify({"count": count})
    except:
        pass
    finally:
        if type(db)==MongoClient:
            db.close()
    return "Error getting count"

# test parameters
@app.route('/p')
def p():
    args = request.args
    al = []
    for arg in args:
        al.append(arg)
        al.append(args[arg])
    return jsonify(al)

@app.route('/homes')
def homes():
    args = request.args
    db=""
    try:
        db = get_db()
        col = db["airbnb_hostings"]

        if col.count() == 0:
            load_into_collection(col)

        locKey = "loc"
        if locKey not in args:
            return "Error in parameters"

        loc = args[locKey]
        loc=loc.lower()
        hostings = col.find({"neighbourhood":{"$eq":loc, "$exists":"true"}})
        hl = []
        for h in hostings:
            del h['_id']
            hl.append(h)
        response = jsonify({"homes": hl})
        response.headers.add("Access-Control-Allow-Origin","*")
        return response
    except: 
        pass
    finally:
        if type(db)==MongoClient:
            db.close()
    return  "Error fetching homes"

@app.route('/hostings')
def get_hostings():
    print("GET host")
    db=""
    try:
        db = get_db()
        print("got the db")
        col = db["airbnb_hostings"]
        hostings = col.find().limit(50)
        hl = []
        for h in hostings:
            del h['_id']
            hl.append(h)
        response = jsonify({"hostings": hl})
        response.headers.add("Access-Control-Allow-Origin","*")
        return response
    except: 
        pass
    finally:
        if type(db)==MongoClient:
            db.close()
    return  "Error retrieving hostings"

def proccesAmenitiesString(amenStr):
    amenList = []
    listDirty = amenStr.split(",")
    for amen in listDirty:
        cleanAmen = amen.replace("\\", "")
        cleanAmen = cleanAmen.replace("[", "")
        cleanAmen = cleanAmen.replace("]", "")
        cleanAmen = cleanAmen.replace('"', "")
        cleanAmen = cleanAmen.strip()
        amenList.append(cleanAmen)
    return amenList


def load_into_collection(col):
    with open('listings_200.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            rowDict = dict(row)
            id = rowDict['id']
            listing_url = rowDict['listing_url']
            scrape_id = rowDict['scrape_id']
            last_scraped = rowDict['last_scraped']
            name = rowDict['name']
            description = rowDict['description']
            neighborhood_overview = rowDict['neighborhood_overview']
            picture_url = rowDict['picture_url']
            host_id = rowDict['host_id']
            host_url = rowDict['host_url']
            host_name = rowDict['host_name']
            host_since = rowDict['host_since']
            host_location = rowDict['host_location']
            host_about = rowDict['host_about']
            host_response_time = rowDict['host_response_time']
            host_response_rate = rowDict['host_response_rate']
            host_acceptance_rate = rowDict['host_acceptance_rate']
            host_is_superhost = rowDict['host_is_superhost']
            host_thumbnail_url = rowDict['host_thumbnail_url']
            host_picture_url = rowDict['host_picture_url']
            host_neighbourhood = rowDict['host_neighbourhood']
            host_listings_count = rowDict['host_listings_count']
            host_total_listings_count = rowDict['host_total_listings_count']
            host_verifications = rowDict['host_verifications']
            host_has_profile_pic = rowDict['host_has_profile_pic']
            host_identity_verified = rowDict['host_identity_verified']
            neighbourhood = rowDict['neighbourhood'].lower()
            latitude = rowDict['latitude']
            longitude = rowDict['longitude']
            property_type = rowDict['property_type']
            room_type = rowDict['room_type']
            accommodates = rowDict['accommodates']
            bathrooms = rowDict['bathrooms']
            bathrooms_text = rowDict['bathrooms_text']
            bedrooms = rowDict['bedrooms']
            beds = rowDict['beds']
            amenities = rowDict['amenities']
            price = rowDict['price']
            minimum_nights = rowDict['minimum_nights']
            maximum_nights = rowDict['maximum_nights']
            minimum_minimum_nights = rowDict['minimum_minimum_nights']
            maximum_minimum_nights = rowDict['maximum_minimum_nights']
            minimum_maximum_nights = rowDict['minimum_maximum_nights']
            maximum_maximum_nights = rowDict['maximum_maximum_nights']
            minimum_nights_avg_ntm = rowDict['minimum_nights_avg_ntm']
            maximum_nights_avg_ntm = rowDict['maximum_nights_avg_ntm']
            calendar_updated = rowDict['calendar_updated']
            has_availability = rowDict['has_availability']
            availability_30 = rowDict['availability_30']
            availability_60 = rowDict['availability_60']
            availability_90 = rowDict['availability_90']
            availability_365 = rowDict['availability_365']
            calendar_last_scraped = rowDict['calendar_last_scraped']
            number_of_reviews = rowDict['number_of_reviews']
            number_of_reviews_ltm = rowDict['number_of_reviews_ltm']
            number_of_reviews_l30d = rowDict['number_of_reviews_l30d']
            first_review = rowDict['first_review']
            last_review = rowDict['last_review']
            review_scores_rating = rowDict['review_scores_rating']
            review_scores_accuracy = rowDict['review_scores_accuracy']
            review_scores_cleanliness = rowDict['review_scores_cleanliness']
            review_scores_checkin = rowDict['review_scores_checkin']
            review_scores_communication = rowDict['review_scores_communication']
            review_scores_location = rowDict['review_scores_location']
            review_scores_value = rowDict['review_scores_value']
            license = rowDict['license']
            instant_bookable = rowDict['instant_bookable']
            calculated_host_listings_count = rowDict['calculated_host_listings_count']
            calculated_host_listings_count_entire_homes = rowDict['calculated_host_listings_count_entire_homes']
            calculated_host_listings_count_private_rooms = rowDict['calculated_host_listings_count_private_rooms']
            calculated_host_listings_count_shared_rooms = rowDict['calculated_host_listings_count_shared_rooms']
            reviews_per_month = rowDict['reviews_per_month']

            amenList = proccesAmenitiesString(amenities)

            col.insert({'id' :id,\
                'listing_url' :listing_url,\
                'scrape_id' :scrape_id,\
                'last_scraped' :last_scraped,\
                'name' :name,\
                'description' :description,\
                'neighborhood_overview' :neighborhood_overview,\
                'picture_url' :picture_url,\
                'host_id' :host_id,\
                'host_url' :host_url,\
                'host_name' :host_name,\
                'host_since' :host_since,\
                'host_location' :host_location,\
                'host_about' :host_about,\
                'host_response_time' :host_response_time,\
                'host_response_rate' :host_response_rate,\
                'host_acceptance_rate' :host_acceptance_rate,\
                'host_is_superhost' :host_is_superhost,\
                'host_thumbnail_url' :host_thumbnail_url,\
                'host_picture_url' :host_picture_url,\
                'host_neighbourhood' :host_neighbourhood,\
                'host_listings_count' :host_listings_count,\
                'host_total_listings_count' :host_total_listings_count,\
                'host_verifications' :host_verifications,\
                'host_has_profile_pic' :host_has_profile_pic,\
                'host_identity_verified' :host_identity_verified,\
                'neighbourhood' :neighbourhood,\
                'latitude' :latitude,\
                'longitude' :longitude,\
                'property_type' :property_type,\
                'room_type' :room_type,\
                'accommodates' :accommodates,\
                'bathrooms' :bathrooms,\
                'bathrooms_text' :bathrooms_text,\
                'bedrooms' :bedrooms,\
                'beds' :beds,\
                'amenities' :amenList,\
                'price' :price,\
                'minimum_nights' :minimum_nights,\
                'maximum_nights' :maximum_nights,\
                'minimum_minimum_nights' :minimum_minimum_nights,\
                'maximum_minimum_nights' :maximum_minimum_nights,\
                'minimum_maximum_nights' :minimum_maximum_nights,\
                'maximum_maximum_nights' :maximum_maximum_nights,\
                'minimum_nights_avg_ntm' :minimum_nights_avg_ntm,\
                'maximum_nights_avg_ntm' :maximum_nights_avg_ntm,\
                'calendar_updated' :calendar_updated,\
                'has_availability' :has_availability,\
                'availability_30' :availability_30,\
                'availability_60' :availability_60,\
                'availability_90' :availability_90,\
                'availability_365' :availability_365,\
                'calendar_last_scraped' :calendar_last_scraped,\
                'number_of_reviews' :number_of_reviews,\
                'number_of_reviews_ltm' :number_of_reviews_ltm,\
                'number_of_reviews_l30d' :number_of_reviews_l30d,\
                'first_review' :first_review,\
                'last_review' :last_review,\
                'review_scores_rating' :review_scores_rating,\
                'review_scores_accuracy' :review_scores_accuracy,\
                'review_scores_cleanliness' :review_scores_cleanliness,\
                'review_scores_checkin' :review_scores_checkin,\
                'review_scores_communication' :review_scores_communication,\
                'review_scores_location' :review_scores_location,\
                'review_scores_value' :review_scores_value,\
                'license' :license,\
                'instant_bookable' :instant_bookable,\
                'calculated_host_listings_count' :calculated_host_listings_count,\
                'calculated_host_listings_count_entire_homes' :calculated_host_listings_count_entire_homes,\
                'calculated_host_listings_count_private_rooms' :calculated_host_listings_count_private_rooms,\
                'calculated_host_listings_count_shared_rooms' :calculated_host_listings_count_shared_rooms,\
                'reviews_per_month' :reviews_per_month
            })
    resp = col.create_index("neighbourhood")

@app.route('/load')
def load_database():
    db=""
    try:
        db = get_db()
        airbnb_col = db["airbnb_hostings"]
        if airbnb_col.count() > 0 :
            return "Collection conatins data...skipping loading"
        load_into_collection(airbnb_col)
        return "Database loaded"
    except:
        pass
    finally:
        if type(db)==MongoClient:
            db.close()
    return "Error loading data into database..."

@app.route('/drop')
def drop_database():
    db=""
    try:
        db = get_db()
        airbnb_col = db["airbnb_hostings"]
        airbnb_col.drop() 
        return "Database dropped"
    except:
        pass
    finally:
        if type(db)==MongoClient:
            db.close()
    return "Error droping database..."

if __name__ == '__main__':
    app.run(debug = True, host = '0.0.0.0', port = 5000)