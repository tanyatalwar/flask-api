from flask import Flask
from flask import Blueprint
from flask_restful import Api
from flask import Flask, jsonify, request
import pymongo
import json
from bson.json_util import dumps

app = Flask(__name__)

client = pymongo.MongoClient('mongodb+srv://prodigal_be_test_01:prodigaltech@test-01-ateon.mongodb.net/sample_training')
db = client.get_database('sample_training')

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/students', methods = ['GET'])
def func_Q1():
    students_data = []
    students_coll = db['students']
    for student in students_coll.find():
        students_data.append({'student_id' : student['_id'], 'student_name' : student['name']})
    return jsonify(students_data)

@app.route('/student/<int:studentid>/classes', methods = ['GET'])
def func_Q2(studentid):
    students_coll = db['students']
    grades_coll = db['grades']
    student = students_coll.find_one({'_id': studentid })
    student_class = grades_coll.find({'student_id': student['_id']})
    students_data = {'student_id': student['_id'], 'student_name': student['name'] , 'classes' : []}
    for class_id in student_class:
        students_data['classes'].append({'class_id' : class_id['class_id']})
    return json.dumps(students_data)

@app.route('/student/<int:studentid>/performance', methods = ['GET'])
def func_Q3(studentid):
    student = db['students'].find_one({'_id': 1})
    student_class = db['grades'].aggregate([{'$match': {'student_id': student['_id']}},{'$project': {'_id': 0, 'class_id': 1, 'total_marks': {'$toInt': {'$sum': '$scores.score'}}}}])
    students_data = {'student_id': student['_id'], 'student_name': student['name'] , 'classes' : []}
    for class_id in student_class:
        students_data['classes'].append(class_id)
    return json.dumps(students_data)

@app.route('/classes', methods = ['GET'])
def func_Q4():
    if request.method == 'GET':
        grades_coll = db['grades']
        class_distinct = grades_coll.distinct("class_id")
        class_id = []
        for classid in class_distinct:
            class_id.append({ 'class_id' : classid })
        return json.dumps(class_id)

@app.route('/class/<int:classid>/students', methods = ['GET'])
def func_Q5(classid):
    if request.method == 'GET':
        students = db['grades'].aggregate([{'$match': {'class_id': classid}},{'$lookup' : {'from': 'students','localField': 'student_id','foreignField': '_id','as': 'student'}},{'$project': {'_id': 0, 'student_id': 1, 'student_name': '$student.name'}}])
        class_data = {'class_id': classid, 'students': []}
        for student in students:
          class_data['students'].append({'student_id' : student['student_id'], 'student_name': student['student_name'][0]})
        return json.dumps(class_data)

@app.route('/class/<int:classid>/performance', methods = ['GET'])
def func_Q6(classid):
    if request.method == 'GET':
        class_data = {'class_id': classid, 'students': []}
        for student in students:
            class_data['students'].append({'student_id' : student['student_id'], 'student_name': student['student_name'][0], 'total_marks': student['total_marks']})
        return json.dumps(class_data)


@app.route('/class/<int:classid>/final-grade-sheet', methods = ['GET'])
def func_Q7(classid):
    students = list(db['grades'].aggregate([
    {'$match': {'class_id': classid}},
    {'$lookup': {
    'from': 'students',
    'localField': 'student_id',
    'foreignField': '_id',
    'as': 'student'
    }},
    {'$project': {'_id': 0, 'student_id': 1, 'scores': 1, 'student_name': '$student.name', 'total_marks': {'$sum': '$scores.score'}}},
    {'$sort': {'total_marks': -1}}
    ]))
    class_data = {'class_id': 1, 'students': []}
    count = len(students)
    for idx, student in enumerate(students):
        details = []
    for score in student['scores']:
        details.append({'type': score['type'], 'marks': int(score['score'])})
        details.append({'type': 'total', 'marks': int(student['total_marks'])})
    if idx+1 < (count / 12):
        class_data['students'].append({'student_id' : student['student_id'], 'student_name': student['student_name'][0], 'details': details, 'grade': 'A'})
    elif idx+1 < (count / 6):
        class_data['students'].append({'student_id' : student['student_id'], 'student_name': student['student_name'][0], 'details': details, 'grade': 'B'})
    elif idx+1 < (count / 4):
        class_data['students'].append({'student_id' : student['student_id'], 'student_name': student['student_name'][0], 'details': details, 'grade': 'C'})
    else:
        class_data['students'].append({'student_id' : student['student_id'], 'student_name': student['student_name'][0], 'details': details, 'grade': 'D'})
    return json.dumps(class_data)


@app.route('/class/<int:classid>/student/<int:studentid>', methods = ['GET'])
def func_Q8(classid,studentid):
    students = db['grades'].aggregate([
    {'$match': {'student_id': studentid, 'class_id': classid }},
    {'$lookup' : {
    'from': 'students',
    'localField': 'student_id',
    'foreignField': '_id',
    'as': 'student'
    }},
    {'$project': {'_id': 0, 'student_id': 1, 'scores': 1, 'student_name': '$student.name', 'total_marks': {'$toInt': {'$sum': '$scores.score'}}}},
    ])
    class_data = {'class_id': classid, 'student_id': studentid, 'student_name': '', 'marks': []}
    for student in students:
        marks = []
    for score in student['scores']:
        marks.append({'type': score['type'], 'marks': int(score['score'])})
        marks.append({'type': 'total', 'marks': student['total_marks']})
    class_data['student_name'] = student['student_name'][0]
    class_data['marks'] = marks
    return json.dumps(class_data)
