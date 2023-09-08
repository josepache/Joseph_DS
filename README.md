# Joseph_DS

### Análisis de Series Temporales de CES
Este proyecto analiza las series temporales de CES proporcionadas por BLS. El objetivo principal es estudiar la evolución de las mujeres en el gobierno a lo largo del tiempo y la evolución de la relación "empleados de producción / empleados supervisores" durante el mismo período.

### Estructura del Proyecto
/scripts: Contiene el script principal main.py que descarga, procesa y almacena los datos en una base de datos PostgreSQL.
/config: Contiene archivos de configuración, como postgrest.conf.

### Cómo ejecutar
1. Asegúrate de tener instalado Python 3 y PostgreSQL.
2. Clona este repositorio.
3. Navega a la carpeta del proyecto y ejecuta el script main.py:

cd path_to_project/Joseph_DS/scripts
python3 main.py

4. Asegúrate de tener configurado y en ejecución PostgREST con el archivo de configuración adecuado

### Endpoints de API
Una vez que PostgREST esté en ejecución, puedes acceder a los siguientes endpoints:

http://localhost:3000/women_in_government: Muestra la evolución de las mujeres en el gobierno a lo largo del tiempo.
http://localhost:3000/prod_supervisor_ratio: Muestra la evolución de la relación "empleados de producción / empleados supervisores" a lo largo del tiempo.

### Contribuciones
Las contribuciones y comentarios son bienvenidos. Por favor, abre un issue o envía un pull request.