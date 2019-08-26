import paho.mqtt.client as mqtt
import sys
import MySQLdb

try:
    print('Conectando...')
    db = MySQLdb.connect(user='<usuario>', passwd='<senhabanco>', host='localhost', port=3306)
    print("conectado")
except:
    print("Falha de conexão com banco de dados!!!")
    print("Encerrando conexão...")
    sys.exit()


def on_connect(client, userdata, flags, rc):
    print("Conectado - resultado do código: " + str(rc))
    client.subscribe("#")

def on_message(mqttc, obj, msg):
    print(msg.topic + " "  + str(msg.payload))
    lista = msg.topic.split("/")
    payload = str(msg.payload)
    b = "b'"
    for i in range(0, len(b)):
        payload = payload.replace(b[i], "")

    try:
        # with db.cursor() as cursor:
        cursor = db.cursor()
        cursor.execute(
            'INSERT INTO mqtt.geladeira (equipamento, tipo_medicao, valor) VALUES (%s, %s, %s)',
            [
                (lista[0]),
                (lista[1]),
                (payload)
            ])
        db.commit()
        cursor.close()
        print("Dados salvos no banco com sucesso")
    except:
        db.rollback()
        print("Ocorreu uma falha na gravação dos dados no banco")
    finally:
        if (db.is_connected()):
            cursor.close()
            db.close()
            print("MySQL conexão fechada")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect("localhost", 1883, 60)
except:
    print("Não pode conectar ao mqtt")
    print("Disconectando...")
    db.close()
    sys.exit()

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("Encerrando!!!")
    db.close()
