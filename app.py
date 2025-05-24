from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://gnuhealth:0928liuyi@@localhost:5432/gnuhealthdb'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Define mapping result model
class MappingResult(db.Model):
    __tablename__ = 'data_mapping_rLIUyi@
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    platform_code = db.Column(db.String(50))
    platform_name = db.Column(db.String(200))
    client_code = db.Column(db.String(50))
    client_name = db.Column(db.String(200))
    match_score = db.Column(db.Float)
    match_status = db.Column(db.String(50))

# Home page
@app.route('/')
def index():
    return render_template('index.html')

# Upload data dictionaries page
@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        # Process uploaded files
        platform_file = request.files['platform_file']
        client_file = request.files['client_file']
        
        # Save files
        platform_path = os.path.join('uploads', platform_file.filename)
        client_path = os.path.join('uploads', client_file.filename)
        platform_file.save(platform_path)
        client_file.save(client_path)
        
        # TODO: Invoke mapping logic
        # This should integrate the DataDictionaryMapper class above
        
        return jsonify({"message": "Files uploaded successfully", "platform_file": platform_file.filename, "client_file": client_file.filename})
    
    return render_template('upload.html')

# View mapping results
@app.route('/results')
def results():
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    # Query results with pagination
    results = MappingResult.query.paginate(page=page, per_page=per_page, error_out=False)
    
    return render_template('results.html', results=results)

# Manual mapping page
@app.route('/manual_mapping/<string:platform_code>')
def manual_mapping(platform_code):
    # Get record for specific platform code
    record = MappingResult.query.filter_by(platform_code=platform_code).first_or_404()
    
    # Get all client code options
    all_client_codes = db.session.query(MappingResult.client_code, MappingResult.client_name).distinct().all()
    
    return render_template('manual_mapping.html', record=record, client_options=all_client_codes)

# Update mapping result
@app.route('/update_mapping', methods=['POST'])
def update_mapping():
    platform_code = request.form.get('platform_code')
    new_client_code = request.form.get('client_code')
    
    # Update record
    record = MappingResult.query.filter_by(platform_code=platform_code).first_or_404()
    client_record = MappingResult.query.filter_by(client_code=new_client_code).first()
    
    if client_record:
        record.client_code = client_record.client_code
        record.client_name = client_record.client_name
        record.match_score = 1.0
        record.match_status = 'Manually matched'
        db.session.commit()
        
        return jsonify({"status": "success", "message": "Mapping updated successfully"})
    
    return jsonify({"status": "error", "message": "Client code not found"})

# Export mapping table
@app.route('/export')
def export():
    # Query all mapping results
    results = MappingResult.query.all()
    
    # Convert to DataFrame
    df = pd.DataFrame([{
        'Platform Code': r.platform_code,
        'Platform Name': r.platform_name,
        'Client Code': r.client_code,
        'Client Name': r.client_name,
        'Match Status': r.match_status
    } for r in results])
    
    # Export to Excel
    export_path = os.path.join('exports', 'mapping_result.xlsx')
    df.to_excel(export_path, index=False)
    
    return jsonify({"status": "success", "file_path": export_path})

if __name__ == '__main__':
    # Create necessary directories
    for dir_name in ['uploads', 'exports']:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
    
    # Create database tables
    with app.app_context():
        db.create_all()
    
    app.run(debug=True, host='0.0.0.0', port=5000)  
