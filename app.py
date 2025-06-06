from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import io
from datetime import datetime
import os
from werkzeug.utils import secure_filename
import json

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Crear directorio de uploads si no existe
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Variable global para almacenar el DataFrame
df_global = None

# Funciones de validación (copiadas de tu archivo condiciones.py)
def errores_generales(df):
    df["Error"] = ""

    mask1 = (
        ((df["Id_Condicion_Establecimiento"] != "C") | (df["Id_Condicion_Servicio"] != "C")) &
        (df["Id_Ups"] == 302101)
    )
    df.loc[mask1, "Error"] = "Condicion establecimiento y Servicio tiene que ser Continuadores"

    mask2 = (df["Codigo_Item"] == "Z019") & (df["Valor_Lab"] != "DNT")
    df.loc[mask2, "Error"] = "EL VALOR LAB TIENE QUE SER DNT"

    mask3 = (df["Codigo_Item"] == "85018") & (df["Valor_Lab"].isna() | (df["Valor_Lab"] == ""))
    df.loc[mask3, "Error"] = "Verificar el numero de Tamizaje"

    return df[df["Error"] != ""].copy()

def errores_adolescente(df):
    df["Error"] = ""

    mask1 = (
        (df["Codigo_Item"] == "99199.26") &
        (df["Valor_Lab"] != "TA") &
        (df["Anio_Actual_Paciente"].between(12, 17))
    )
    df.loc[mask1, "Error"] = "VERIFICAR SUPLEMENTACION EN ADOLESCENTES QUE NO SEAN TA"

    return df[df["Error"] != ""].copy()

def errores_obstetricia(df):
    df["Error"] = ""

    mask1 = (df["Codigo_Item"] == "99208.13") & (df["Tipo_Diagnostico"] == "R") & (df["Valor_Lab"] != "4")
    df.loc[mask1, "Error"] = "El codigo 99208.13 con DX R solo acepta el campo LAB con valor 4"

    mask2 = (df["Codigo_Item"] == "99208.13") & (df["Tipo_Diagnostico"] == "D") & (df["Valor_Lab"] != "1")
    df.loc[mask2, "Error"] = "El codigo 99208.13 con DX D solo acepta el campo LAB con valor 1 o cambiar el Diagnostico a R SI EL LAB ES 4"

    mask3 = (df["Codigo_Item"] == "99208.02") & (df["Tipo_Diagnostico"] == "D") & (df["Valor_Lab"] != "10")
    df.loc[mask3, "Error"] = "El codigo 99208.02 con DX D solo acepta el campo LAB con valor 10 si el valor lab es 30, corregir DX R"

    mask4 = (df["Codigo_Item"] == "99208.02") & (df["Tipo_Diagnostico"] == "R") & (df["Valor_Lab"] != "30")
    df.loc[mask4, "Error"] = "El codigo 99208.02 con DX R solo acepta el campo LAB con valor 30 si el valor es 10 poner D"

    mask5 = (df["Codigo_Item"] == "99208.06") & (df["Tipo_Diagnostico"] == "R") & (df["Valor_Lab"] != "30")
    df.loc[mask5, "Error"] = "El codigo 99208.06 con DX R solo acepta el campo LAB con valor 30"

    mask6 = (df["Codigo_Item"] == "99208.04") & (df["Tipo_Diagnostico"].isin(["D", "R"])) & (df["Valor_Lab"] != "1")
    df.loc[mask6, "Error"] = "El codigo 99208.04 solo acepta el campo LAB con valor 1"

    mask7 = (df["Codigo_Item"] == "99208.05") & (df["Tipo_Diagnostico"].isin(["D", "R"])) & (df["Valor_Lab"] != "1")
    df.loc[mask7, "Error"] = "El codigo 99208.05 solo acepta el campo LAB con valor 1"

    mask8 = (df["Codigo_Item"] == "99208.06") & (df["Tipo_Diagnostico"] == "D") & (df["Valor_Lab"] != "10")
    df.loc[mask8, "Error"] = "El codigo 99208.06 con DX D solo acepta el campo LAB con valor 10"

    mask9 = (df["Codigo_Item"] == "92100") & (~df["Valor_Lab"].isin(["N", "A"]))
    df.loc[mask9, "Error"] = "EL Valor_Lab tiene que ser N o A"

    mask10 = (
        df["Codigo_Item"].isin(["86703", "87342", "86780", "87340", "86703.01", "86703.02", "86318.01", "86803.01"]) &
        (df["Tipo_Diagnostico"] == "D") &
        (~df["Valor_Lab"].isin(["RP", "RN"]))
    )
    df.loc[mask10, "Error"] = "El campo LAB debe ser RN= Resultado Negativo o RP= Resultado Positivo"

    mask11 = (df["Codigo_Item"] == "59401.06") & (df["Valor_Lab"].isna())
    df.loc[mask11, "Error"] = "Campo Lab no debe de estar vacio"

    mask12 = (
        ((df["Codigo_Item"] == "80055.01") & ((df["Valor_Lab"] != "1") | (df["Valor_Lab"].isna()))) |
        ((df["Codigo_Item"] == "80055.02") & ((df["Valor_Lab"] != "2") | (df["Valor_Lab"].isna())))
    )
    df.loc[mask12, "Error"] = "CORREGIR PRIMERA BATERIA 80055.01 CON LAB 1 Y SEGUNDA BATERIA 80055.02 CON LAB 2"

    mask13 = df["Codigo_Item"].isin(["86703.01", "86703.02", "86780", "86318.01", "87342"]) & \
             (~df["Valor_Lab"].isin(["RN", "RP"]))
    df.loc[mask13, "Error"] = "Valor_Lab solo debe de Tener RN y RP"

    mask14 = df["Codigo_Item"].isin(["88141.01", "99386.03"]) & \
             ((~df["Valor_Lab"].isin(["N", "A"])) | df["Valor_Lab"].isna())
    df.loc[mask14, "Error"] = "VALOR LAB Normal o Anormal"

    return df[df["Error"] != ""].copy()

def errores_dental(df):
    df["Error"] = ""

    mask1 = df["Codigo_Item"].isin([
        "D5110", "D5213", "D5120", "D5214", "D5130", "D5225", "D5140",
        "D5226", "D5211", "D5860", "D5212", "D5861"
    ]) & (df["Valor_Lab"].isna())
    df.loc[mask1, "Error"] = "El Valor_Lab no puede estar vacio"

    mask2 = df["Codigo_Item"].isin(["D1310", "D1330"]) & (df["Valor_Lab"].isna())
    df.loc[mask2, "Error"] = "El Valor_Lab no puede estar vacio"

    return df[df["Error"] != ""].copy()

def errores_inmunizaciones(df):
    df["Error"] = ""

    mask1 = df["Codigo_Item"] == "90676"
    df.loc[mask1, "Error"] = "Vacuna Antirrabica es 90675"

    return df[df["Error"] != ""].copy()

# Columnas base para mostrar
columnas_base = [
    "Id_Cita", "Anio", "Mes", "Fecha_Atencion", "Lote", "Num_Pag", "Num_Reg", "Id_Ups",
    "Descripcion_Ups", "Nombre_Establecimiento",
    "Numero_Documento_Paciente", "Nombres Completo Paciente",
    "Fecha_Nacimiento_Paciente", "Genero",
    "Numero_Documento_Personal", "Nombres Completo Personal",
    "Id_Condicion_Establecimiento", "Id_Condicion_Servicio",
    "Edad_Reg", "Mes_Actual_Paciente", "Anio_Actual_Paciente", "Tipo_Diagnostico",
    "Valor_Lab", "Codigo_Item", "id_ups", "Hemoglobina", "Observaciones", "Error"
]

def calcular_edad_formato(fecha_nac, fecha_atencion):
    """Calcula la edad en formato Años-Meses-Días"""
    if pd.isna(fecha_nac) or pd.isna(fecha_atencion):
        return ""
    delta = fecha_atencion - fecha_nac
    total_dias = delta.days
    años = total_dias // 365
    meses = (total_dias % 365) // 30
    dias = (total_dias % 365) % 30
    return f"{años}A-{meses}M-{dias}D"

def formatear_fechas(df):
    """Formatea las fechas a DD/MM/YYYY"""
    for col in ["Fecha_Atencion", "Fecha_Nacimiento_Paciente"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
    return df

def procesar_dataframe(df):
    """Procesa el DataFrame agregando campos calculados"""
    # Crear nombres completos
    df["Nombres Completo Paciente"] = (
        df.get("Apellido_Paterno_Paciente", pd.Series()).fillna('') + " " +
        df.get("Apellido_Materno_Paciente", pd.Series()).fillna('') + " " +
        df.get("Nombres_Paciente", pd.Series()).fillna('')
    ).str.strip()
    
    df["Nombres Completo Personal"] = (
        df.get("Apellido_Paterno_Personal", pd.Series()).fillna('') + " " +
        df.get("Apellido_Materno_Personal", pd.Series()).fillna('') + " " +
        df.get("Nombres_Personal", pd.Series()).fillna('')
    ).str.strip()
    
    # Calcular edad
    df["Edad_Reg"] = df.apply(
        lambda row: calcular_edad_formato(
            row.get("Fecha_Nacimiento_Paciente"), 
            row.get("Fecha_Atencion")
        ), axis=1
    )
    
    return df

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    global df_global
    
    if 'file' not in request.files:
        return jsonify({'error': 'No se seleccionó ningún archivo'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No se seleccionó ningún archivo'}), 400
    
    if file and file.filename.lower().endswith('.xlsx'):
        try:
            # Leer el archivo Excel
            df = pd.read_excel(file)
            
            # Procesar el DataFrame
            df = procesar_dataframe(df)
            df = formatear_fechas(df)
            
            # Guardar en variable global
            df_global = df
            
            # Preparar datos para enviar al frontend (solo primeras 100 filas)
            cols = [c for c in columnas_base[:-1] if c in df.columns]  # Excluir "Error" inicialmente
            df_muestra = df[cols].head(100)
            df_muestra = df_muestra.fillna('')
            
            # Convertir a formato JSON
            data = {
                'columns': cols,
                'data': df_muestra.values.tolist(),
                'total_records': len(df),
                'shown_records': len(df_muestra)
            }
            
            return jsonify({
                'success': True,
                'message': 'Archivo cargado correctamente',
                'data': data
            })
            
        except Exception as e:
            return jsonify({'error': f'Error al procesar el archivo: {str(e)}'}), 500
    else:
        return jsonify({'error': 'Solo se permiten archivos Excel (.xlsx)'}), 400

@app.route('/filter/<filter_type>')
def apply_filter(filter_type):
    global df_global
    
    if df_global is None:
        return jsonify({'error': 'No hay datos cargados'}), 400
    
    try:
        # Aplicar el filtro correspondiente
        filter_functions = {
            'generales': errores_generales,
            'dental': errores_dental,
            'adolescente': errores_adolescente,
            'obstetricia': errores_obstetricia,
            'inmunizaciones': errores_inmunizaciones
        }
        
        if filter_type not in filter_functions:
            return jsonify({'error': 'Tipo de filtro no válido'}), 400
        
        df_filtrado = filter_functions[filter_type](df_global.copy())
        
        if df_filtrado.empty:
            return jsonify({
                'success': True,
                'message': 'No se encontraron errores',
                'data': {
                    'columns': [],
                    'data': [],
                    'total_records': 0,
                    'shown_records': 0
                }
            })
        
        # Formatear fechas
        df_filtrado = formatear_fechas(df_filtrado)
        
        # Preparar datos para el frontend
        cols = [c for c in columnas_base if c in df_filtrado.columns]
        df_muestra = df_filtrado[cols].head(100)
        df_muestra = df_muestra.fillna('')
        
        data = {
            'columns': cols,
            'data': df_muestra.values.tolist(),
            'total_records': len(df_filtrado),
            'shown_records': len(df_muestra)
        }
        
        # Guardar datos filtrados para descarga
        app.config['df_filtrado'] = df_filtrado
        
        return jsonify({
            'success': True,
            'message': f'Filtro aplicado: {len(df_filtrado)} errores encontrados',
            'data': data
        })
        
    except Exception as e:
        return jsonify({'error': f'Error al aplicar filtro: {str(e)}'}), 500

@app.route('/download')
def download_excel():
    try:
        df_filtrado = app.config.get('df_filtrado')
        if df_filtrado is None or df_filtrado.empty:
            return jsonify({'error': 'No hay datos filtrados para descargar'}), 400
        
        # Crear archivo Excel en memoria
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            cols = [c for c in columnas_base if c in df_filtrado.columns]
            df_filtrado[cols].to_excel(writer, index=False, sheet_name="ErroresFiltrados")
            
            # Ajustar ancho de columnas
            worksheet = writer.sheets["ErroresFiltrados"]
            for i, col in enumerate(cols):
                max_len = max(df_filtrado[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, min(max_len, 50))
        
        output.seek(0)
        
        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"ErroresFiltrados_{timestamp}.xlsx"
        
        return send_file(
            io.BytesIO(output.read()),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({'error': f'Error al generar archivo: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)