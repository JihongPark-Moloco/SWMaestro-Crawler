from google.cloud import vision  
import psycopg2 as pg2 
def findVideoIdx(uri):
    conn = None
    idx=None
    
    try:
        conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="222.112.206.190",
                           port="5432")
        cur = conn.cursor()
        cur.execute(f"SELECT idx from video where thumbnail_url = '{uri}'")
        rows = cur.fetchall()
        newrows = [row[0] for row in rows]
        idx=newrows[0]

    except Exception as e:
        print("postgresql database conn error")
        print(e)
    conn.close()
    return idx
    
    
       
def localize_objects_uri(uri,idx):
    """Localize objects in the image on Google Cloud Storage

    Args:
    uri: The path to the file in Google Cloud Storage (gs://...)
    """
    print(uri)
    client = vision.ImageAnnotatorClient()

    image = vision.types.Image()
    image.source.image_uri = uri
    print(uri,idx)
    objects = client.object_localization(
        image=image).localized_object_annotations

    print('Number of objects found: {}'.format(len(objects)))
    conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="222.112.206.190",
                               port="5432")
    for object_ in objects:
        try:
            cur = conn.cursor()
            cur.execute(f"insert into thumbnail_objects (video_idx, object,object_score) values({idx},'{object_.name}',{object_.score})")
            conn.commit()
        except Exception as e:
            print("postgresql database conn error")
            print(e)
    conn.close() 
    
    
        
        

def detect_logos_uri(uri,idx):
    """Detects logos in the file located in Google Cloud Storage or on the Web.
    """
    client = vision.ImageAnnotatorClient()
    image = vision.types.Image()
    image.source.image_uri = uri

    response = client.logo_detection(image=image)
    logos = response.logo_annotations
    print('Logos:')
    conn = pg2.connect(database="createtrend", user="muna", password="muna112358!", host="222.112.206.190",
                               port="5432")
    for logo in logos:
        try:
            logo.description
            cur = conn.cursor()
            cur.execute(f"insert into thumbnail_logo (video_idx, logo) values({idx},'{logo.description}')")
            conn.commit()
        except Exception as e:
            print("postgresql database conn error")
            print(e)
    conn.close()
    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))

def main(uri):
    try:
        idx=findVideoIdx(uri)
        localize_objects_uri(uri,idx)
        detect_logos_uri(uri,idx)
    except:
        print("Error")
    

    
    

#!/usr/bin/env python
