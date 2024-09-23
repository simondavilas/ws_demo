from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import time
import csv
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import re

headers = {
  'authority': 'el.soccerway.com',
  'method': 'GET',
  'path': '/national/colombia/primera-a/2024/clausura/r80128/',
  'scheme': 'https',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-encoding': 'gzip, deflate, br, zstd',
  'accept-language': 'en-US,en;q=0.9,es-CO;q=0.8,es-US;q=0.7,es;q=0.6',
  'cache-control': 'max-age=0',
  'referer': 'https://el.soccerway.com/',
  'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
}

cookies = {
  '_ga': 'GA1.3.2005189154.1725752462',
  '_gid': 'GA1.3.1895285436.1725752462',
  'OptanonAlertBoxClosed': '2024-09-07T23:41:05.475Z',
  'eupubconsent-v2': 'CQElqogQElqogAcABBENBGFgAAAAAAAAACiQAAAAAAAA.YAAAAAAAAAAA',
  '_ga_K2ECMCJBFQ': 'GS1.1.1725752462.1.1.1725752913.0.0.0',
  '_ga_SQ24F7Q7YW': 'GS1.1.1725752463.1.1.1725752913.0.0.0',
  'OptanonConsent': 'isGpcEnabled=0&datestamp=Sat+Sep+07+2024+18%3A48%3A38+GMT-0500+(hora+est%C3%A1ndar+de+Colombia)&version=202310.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=ce7f3633-89ad-4f3f-88ea-d1b50164dbd1&interactionCount=1&landingPath=NotLandingPage&groups=C0002%3A0%2CC0001%3A1%2CC0004%3A0%2CV2STACK42%3A0&geolocation=CO%3BDC&AwaitingReconsent=false',
  '_ga_RHE1E44KQH': 'GS1.3.1725752462.1.1.1725752918.58.0.0',
  'nol_fpid': 'rb4um3hpbhx0mvfn5kerykctbvoa01725752462|1725752462973|1725752919116|1725752919510'
}

def create_connection():
  connection = None
  try:
    connection = mysql.connector.connect(
      host='localhost',
      database='estadisticasfpc',
      user='root',
      password=''
    )
    print("Conexión a MySQL DB exitosa")
  except Error as e:
      print(f"Error al conectar a MySQL: {e}")
  return connection

def create_table(connection):
  create_table_query = """
  CREATE TABLE IF NOT EXISTS soccerway_primera_a_matches (
    id INT AUTO_INCREMENT PRIMARY KEY,
    torneo VARCHAR(255),
    año INT(4),
    semestre VARCHAR(255),
    fase VARCHAR(255),
    jornada INT,
    match_date DATE,
    team_home VARCHAR(255),
    home_score INT,
    score VARCHAR(10),
    team_away VARCHAR(255),
    away_score INT,
    match_url VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
  """
  try:
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    print("Tabla creada exitosamente")
  except Error as e:
    print(f"Error al crear la tabla: {e}")

def insert_or_update_match(connection, torneo, año, semestre, fase, jornada, match_date, team_home, home_score, score, team_away, away_score, match_url):
  insert_query = """
  INSERT INTO soccerway_primera_a_matches (torneo, año, semestre, fase, jornada, match_date, team_home, home_score, score, team_away, away_score, match_url)
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
  ON DUPLICATE KEY UPDATE
  torneo = VALUES(torneo),
  año = VALUES(año),
  semestre = VALUES(semestre),
  fase = VALUES(fase),
  jornada = VALUES(jornada),
  match_date = VALUES(match_date),
  team_home = VALUES(team_home),
  home_score = VALUES(home_score),
  score = VALUES(score),
  team_away = VALUES(team_away),
  away_score = VALUES(away_score)
  """
  try:
    cursor = connection.cursor()
    cursor.execute(insert_query, (torneo, año, semestre, fase, jornada, match_date, team_home, home_score, score, team_away, away_score, match_url))
    connection.commit()
    if cursor.rowcount == 1:
      print(f"Nuevo partido insertado: {team_home} vs {team_away}")
    elif cursor.rowcount == 2:
      print(f"Partido actualizado: {team_home} vs {team_away}")
    else:
      print(f"No se realizaron cambios para: {team_home} vs {team_away}")
  except Error as e:
    print(f"Error al insertar el partido: {e}")

def convert_date(date_string):
  # Usar expresión regular para extraer la fecha numérica
  match = re.search(r'(\d{2}/\d{2}/\d{4})', date_string)
  if not match:
    return "Formato de fecha no válido"
  
  # Extraer la fecha encontrada
  date_str = match.group(1)
  
  # Convertir la fecha al formato deseado
  try:
    date_obj = datetime.strptime(date_str, '%d/%m/%Y')
    return date_obj.strftime('%Y-%m-%d')
  except ValueError:
    return "Error al convertir la fecha"

def select_round_option(driver, index, id):
  max_attempts = 3
  for attempt in range(max_attempts):
    try:
      dropdown_ronda = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, id))
      )
      select_ronda = Select(dropdown_ronda)
      select_ronda.select_by_index(index)
      time.sleep(2)  # Esperar a que la página se actualice
      return True
    except StaleElementReferenceException:
      if attempt < max_attempts - 1:
        print(f"Elemento obsoleto, reintentando... (intento {attempt + 1})")
        time.sleep(2)
      else:
        print(f"No se pudo seleccionar la opción después de {max_attempts} intentos")
        return False

def setup_driver():
  chrome_options = Options()
  chrome_options.add_argument("--headless")  # Ejecutar en modo headless
  # service = Service('path/to/chromedriver')  # Asegúrate de especificar la ruta correcta
  # driver = webdriver.Chrome(service=service, options=chrome_options)
  # driver = webdriver.Chrome(options=chrome_options)
  driver = webdriver.Chrome()

  return driver

def scrape_soccerway_colombia(url):
  driver = setup_driver()
  # connection = create_connection()

  # if connection is not None:
  #   create_table(connection)

  # Crear un archivo CSV para guardar los resultados
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  url_parts = url.split('/')
  filename = f"soccerway_results_{url_parts[5]}_{url_parts[6]}_{url_parts[7]}_{timestamp}.csv"

  with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Escribir la cabecera del CSV
    csv_writer.writerow(['torneo', 'año', 'semestre', 'fase', 'jornada', 'match_date', 'team_home', 'home_score', 'score', 'team_away', 'away_score', 'match_url'])
    try:
      driver.get(url)

      time.sleep(5)
      # Esperar a que la página se cargue completamente
      WebDriverWait(driver, 30).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
      )
      for cookie_name, cookie_value in cookies.items():
        driver.add_cookie({'name': cookie_name, 'value': cookie_value, 'domain': 'el.soccerway.com'})
      driver.refresh()
      time.sleep(5)

      dropdown_año = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "season_id_selector"))
      )
      select_año = Select(dropdown_año)
      options_año = select_año.options
      # for index_año, option_año in enumerate(options_año, start=1):
      for index_año, option_año in enumerate(options_año):
        print(f"Seleccionando opción {index_año + 1} de {len(options_año)} AÑO")
        if select_round_option(driver, index_año, "season_id_selector"):
          print(f"Terminado scraping del año {index_año + 1}")
          time.sleep(2)
        else:
          print(f"Saltando año {index_año + 1} debido a un error")
        # select_año.select_by_index(index_año)
        time.sleep(3)

        dropdown_ronda = WebDriverWait(driver, 10).until(
          EC.presence_of_element_located((By.ID, "round_id_selector"))
        )
        select_ronda = Select(dropdown_ronda)
        options_ronda = select_ronda.options

        # time.sleep(10)
        for index_ronda, option_ronda in enumerate(options_ronda):
          print(f"Seleccionando opción {index_ronda + 1} de {len(options_ronda)} RONDA")
          if select_round_option(driver, index_ronda, "round_id_selector"):
            print(f"Terminado scraping de la ronda {index_ronda + 1}")
            time.sleep(2)
          else:
            print(f"Saltando ronda {index_ronda + 1} debido a un error")
          print(driver.current_url)
          url_parts = driver.current_url.split('/')
          torneo = url_parts[5]
          año = url_parts[6]
          if url_parts[7] != 'apertura' and url_parts[7] != 'apertura---quadrangular' and url_parts[7] != 'clausura' and url_parts[7] != 'clausura---quadrangular':
            semestre = url_parts[8]
          else:
            semestre = url_parts[7]
          # currente leafe o current expanded
          print(torneo)
          print(año)
          print(semestre)
          if año == "2024" and semestre == "clausura---quadrangular":
          # if año == "2024":
            options = []
          elif semestre == 'final-stages':
            options = [1]
          else:
            # Esperar a que el menú desplegable esté presente
            dropdown = WebDriverWait(driver, 10).until(
              EC.presence_of_element_located((By.ID, "page_competition_1_block_competition_matches_summary_9_page_dropdown"))
            )
            
            # Crear un objeto Select
            select = Select(dropdown)
            
            # Obtener todas las opciones disponibles
            options = select.options
          for index, option in enumerate(options):
            print(f"Seleccionando opción {index + 1} de {len(options)}")
            if semestre != 'final-stages':
              select.select_by_index(index)
            
            # Esperar a que la página se actualice
            time.sleep(3)
            
            # Obtener el HTML actualizado
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            match_table = soup.find_all('table', class_='matches')
            if match_table:
              semestre_list = soup.find('li', class_='current leaf')
              if semestre_list:
                semestre_text = semestre_list.find('a').get_text(strip=True)
              else:
                semestre_text = soup.find('li', class_='current expanded').find('a').get_text(strip=True)
              match_index = 0
              for table in match_table:
                if semestre == 'final-stages':
                  semestre_final = soup.find_all('span', class_='header-label-2')
                  semestre_final_text = semestre_final[match_index].get_text(strip=True).split(' - ')[0] if semestre_final else 'Error'
                  fase = semestre_final[match_index].get_text(strip=True).split(' - ')[1] if semestre_final else 'Error'
                else: 
                  if "Quadrangular" in semestre_text or 'Semifinales' in semestre_text:
                    fase = "Cuadrangulares"
                    semestre_final_text = semestre_text.split(' - ')[0]
                  elif semestre_final_text != "Etapas finales":
                    semestre_final_text = semestre_text
                    fase = "Todos contra todos"
                print(semestre_final_text)
                print(fase)
                tbody = table.find('tbody')
                rows = tbody.find_all('tr')
                current_date = None
                for row in rows:
                  if index > 1 and semestre == 'final-stages':
                    index = 0
                  if 'no-date-repetition-new' in row.get('class',[]):
                    # print("FECHAS")
                    # print(row)
                    # print("-------------------------------------")
                    date_cell = row.find('td', class_='date')
                    # print(date_cell)
                    if date_cell:
                      date_span = date_cell.find('span', class_='timestamp')
                      if date_span:
                        current_date = date_span.get_text(strip=True) if date_span else 'Fecha no disponible'
                      else:
                        current_date = date_cell.get_text(strip=True) if date_cell else 'Fecha no disponible'
                  elif 'match' in row.get('class',[]):
                    # print("MATCHES")
                    # print(row)
                    # print("***************************")
                    if current_date:
                      teams = row.find_all('td', class_='team')
                      team_a = teams[0].text.strip() if len(teams) > 0 else 'Error'
                      team_b = teams[1].text.strip() if len(teams) > 1 else 'Error'
                      score_time = row.find('td', class_='score-time')
                      if score_time:
                        score_link = score_time.find('a')
                        score = score_link.find('span', class_='extra_time_score')
                        score_text = score.get_text(strip=True) if score else 'Vs'
                        score_url = 'https://el.soccerway.com' + score_link['href'] if score_link else 'URL no disponible'
                        if score_text != 'Vs':
                          home_score = score_text.split('-')[0]
                          away_score = score_text.split('-')[1]
                        else:
                          home_score = None
                          away_score = None
                        # Escribir en el CSV
                        date_db = convert_date(current_date)
                        csv_writer.writerow([torneo, año, semestre_final_text, fase, index + 1, date_db, team_a, home_score, score_text, team_b, away_score, score_url])
                        
                        # Insertar en la base de datos
                        # insert_or_update_match(connection, torneo, año, semestre_final_text, fase, index + 1, date_db, team_a, home_score, score_text, team_b, away_score, score_url)
                        if semestre == 'final-stages':
                          index = index + 1
                        print(f"Fecha: {current_date}")
                        print(f"{team_a} {score_text} {team_b}")
                        print(f"URL: {score_url}")
                        print("---------------")
                      else:
                        print(f"ERROR: No se encontró información del resultado para el partido {team_a} vs {team_b}")
                    else:
                      print("ERROR: Partido sin fecha asociada")
                if semestre == 'final-stages':
                  match_index = match_index + 1
            else:
              print("No se encontró la tabla de partidos")
                
            print(f"Terminado scraping de la página {index + 1}")
            time.sleep(2)
            print("==================================")
    except Exception as e:
      print(f"Se produjo un error: {str(e)}")
      driver.save_screenshot("error_screenshot.png")

    finally:
      driver.quit()
      if connection.is_connected():
        connection.close()
        print("Conexión a MySQL cerrada")
  # print(f"Los resultados han sido guardados en {filename}")

# Uso de la función
# url = 'https://el.soccerway.com/national/colombia/primera-a/2024/clausura/r80128/'
url = 'https://el.soccerway.com/national/colombia/primera-a/2024/apertura/r80125/'
scrape_soccerway_colombia(url)