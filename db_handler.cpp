#include "DB_Logic_Module.h"

namespace {
  
    //получение времени формата 2021-09-30 00:00:00  
    std::string Stamp2Time(long long timestamp)
    {
        int ms = timestamp % 1000;//take milliseconds
        time_t tick = (time_t)(timestamp / 1000);//conversion time
        struct tm tm;
        char s[40];
        tm = *localtime(&tick);
        strftime(s, sizeof(s), "%Y-%m-%d %H:%M:%S", &tm);
        std::string str(s);
        return str;
    }

    // получение текущего времени и перевод в timestamp
    std::string GetTimeStamp()
    {
        auto timeNow = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
        long long timestamp = timeNow.count();
        return Stamp2Time(timestamp);
    }

    // получение уникального индекса месяца
    int get_month_index() {
        auto timeNow = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
        long long timestamp = timeNow.count();
        uint64_t time_value = timestamp;
        time_t t = time_value / 1000;
        struct tm* timeinfo;

        //time(&t);
        timeinfo = localtime(&t);

        int month_index = (1900 + timeinfo->tm_year - 1970) * 12 + (timeinfo->tm_mon + 1);
        return month_index;
    }
    
  //замена даты на текущую. время не трогаем
    void replace_date_to_nowaday(QString& src)
    {
        QDate td = QDate::currentDate();

        if (!src.isEmpty())
            src.replace(0, td.toString(Qt::ISODate).size(), td.toString(Qt::ISODate));	
    }

}
// end of namespace



bool DB_Connection::set_connect()
{
    this->m_depmine = new DBCon();
    this->m_depminearch = new DBCon();

    this->m_depmine->set_host(this->m_ip.toStdString());
    this->m_depmine->set_port(this->m_port.toStdString());
    this->m_depmine->set_user(this->m_username.toStdString());
    this->m_depmine->set_passwd(this->m_pswd.toStdString());

    this->m_depminearch->set_host(this->m_ip.toStdString());
    this->m_depminearch->set_port(this->m_port.toStdString());
    this->m_depminearch->set_user(this->m_username.toStdString());
    this->m_depminearch->set_passwd(this->m_pswd.toStdString());

    this->m_srvDBdepminearchConn = new DBConnection();
    this->m_srvDBdepmineConn = new DBConnection();
    if (!this->m_srvDBdepmineConn->open(this->m_depmine, this->m_db_conf_name.toStdString()))
    {
        m_connect_conf=false;
    }

    if (!this->m_srvDBdepminearchConn->open(this->m_depminearch, this->m_db_arch_name.toStdString()))
    {
        //не удалось установить коннект с двумя БД... значит работаем автономно
        if (!m_connect_conf){
            emit BdNoConnection();
            m_connect_arch=false;
            return false;
        }
    }
    this->m_connect_conf = true;
    this->m_connect_arch = true;
    return true;
}

// по таймеру приходим восстанавливать связь в БД
void DB_Connection::reset_connect()
{
    if(!this->m_srvDBdepmineConn->checkConnection()){
        m_connect_conf=false;
        emit BdNoConnection();
        return;
    }
    if(!this->m_srvDBdepminearchConn->checkConnection()){
        m_connect_arch=false;
        emit BdNoConnection();
        return;
    }

    m_connect_conf = true;
    m_connect_arch = true;
    emit BdConnectionOk();
}

void DB_Connection::set_IP(const QString& ip)
{
    this->m_ip = ip;
}

void DB_Connection::set_Port(const QString& port)
{
    this->m_port = port;
}

void DB_Connection::set_db_arch_name(const QString& arch_name)
{
    this->m_db_arch_name = arch_name;
}

void DB_Connection::set_db_conf_name(const QString& conf)
{
    this->m_db_conf_name = conf;
}

void DB_Connection::set_user(const QString& username)
{
    this->m_username = username;
}

void DB_Connection::set_password(const QString& pswd)
{
    this->m_pswd = pswd;
}

// создание таблицы service_shifts
bool DB_Connection::add_chargingFailure_table(const int &month_index) const
{
    PGresult* res = NULL;
    this->m_srvDBdepmineConn->requst(res, "create table IF not exists service_shifts (uid_shift serial, name text, time_open bigint, time_added timestamp);");
    return this->m_srvDBdepminearchConn->requst(res, "create table IF not exists charge_station_log_" + std::to_string(month_index) + "(uid serial, id_app int, type text, descr text, status_code int, time timestamp);");										//PQexec(srvDBConn->get_db(), "create table if not exists t(id int, name text);");
}

//не используется... архив
bool DB_Connection::is_shift_table_empty(const QString& table_name) const 
{
   QString req = "SELECT * FROM " + table_name;
   PGresult* res = NULL;
   res = select_from_conf(req.toStdString());

   int rec_count = PQntuples(res);
   return rec_count ? false : true;
 }

//попытка добавить запись... в случае false - обрыв связи с БД
bool DB_Connection::add_ChargingFailure_column(const QString& req, int& month_index) const
{
    PGresult* res = NULL;
    add_chargingFailure_table(month_index);
    return this->m_srvDBdepminearchConn->requst(res, req.toStdString());
}

PGresult* DB_Connection::select_from_conf(const std::string& req) const
{
    PGresult* res=NULL;
    res=srvDBdepmineConnOBJ()->select(req);
    return res;
}

PGresult* DB_Connection::select_from_arch(const std::string& req) const
{
    PGresult* res = NULL;
    res = m_srvDBdepminearchConn->select(req);
    return res;
}

//отправка запроса на создание сменв
bool DB_Connection::add_shift_column(const QString& req) const
{
    PGresult* res = NULL;
    return this->m_srvDBdepmineConn->requst(res, req.toStdString());
}

// запрос на поиск type and id_app по серийнику в зависимости от типа устройства
void DB_Connection::make_select_conf(const std::string& table, const std::string& row, int& id_app, QString& type, const quint32 a_serial) const {

        std::string select_req;
        PGresult* res = NULL;

        select_req = "SELECT * FROM " + table + " WHERE sn='" + std::to_string(a_serial) + "'";
        res = select_from_conf(select_req);

        int rec_count = PQntuples(res);
        if (res != nullptr && rec_count) {    //нашли
            std::string id_app_str;
            id_app_str = PQgetvalue(res, 0, 0);	
            id_app = std::atoi(id_app_str.c_str());   //получили ИД
            std::string tp = PQgetvalue(res, 0, 1);	

            PQclear(res);

            select_req = "SELECT * FROM " + row + " type_mark WHERE uid_type='" + tp + "'";     // новый запрос, узнаем тип
            res = select_from_conf(select_req);
            if (res != nullptr && rec_count) {
                type = PQgetvalue(res, 0, 1);	      
            }

            PQclear(res);
        }
        else PQclear(res);
}

// архив
bool DB_Connection::is_charging_table_empty(const QString& table_name, int & maxCount) const
{
    QString req = "SELECT * FROM " + table_name; //(SELECT MAX(uid) + 1 FROM charge_station_log_%2)
    PGresult* res = NULL;

    res = select_from_arch(req.toStdString());

    int rec_count = PQntuples(res);
    maxCount = rec_count;
    return rec_count ? false : true;
}

void DB_Connection::insert_into_charging_table(const QString& type, const QString& msg, const quint32 a_serial, const quint32 a_value) const
{
    std::string tm;
    QString req, table_name, tp, time;
    int id_app = 0;
    int month_index = get_month_index();
    table_name = QString("charge_station_log_%1").arg(month_index);
    tp = type;

    //получаем timestamp время
    tm = GetTimeStamp();
    time = tm.c_str();

    //в зависимости от типа - необходимо получить id_app / type 
    if (type.contains("Светильник")) {
        make_select_conf(std::string("devices"), std::string("device_type"), id_app, tp, a_serial);
    }
    else if (type.contains("Метка")) {
        make_select_conf(std::string("mark"), std::string("type_mark"), id_app, tp, a_serial);
    }
    else if (type.contains("ExLUCH-CHRG05")) {
        make_select_conf(std::string("luch"), std::string("type_luch"), id_app, tp, a_serial);
    }


    req = QString("insert into charge_station_log_%1 (id_app, type, descr, status_code, time) values (%2, '%3', '%4', %5,'%6');").arg(month_index).arg(id_app).arg(tp).arg(msg).arg(a_value).arg(tm.c_str());

    //неудачное добавление... запоминаем...при реконнекте снова добавим запись
     if ( !add_ChargingFailure_column(req, month_index))
     {
         req = QString("'%1''%2''%3''%4''%5''%6'").arg(type).arg(msg).arg(a_serial).arg(a_value).arg(month_index).arg(time);
         emit BdNoConnectionWriteToFile(req);
     }
}

bool DB_Connection::insert_into_shifting_table(const QString& nowaday) const
{
    //описание полей таблицы
    struct shift_timetable {
        uint32_t uid;
        QString name;
        QString time_open;
    };

    QVector <shift_timetable> m_timetable_values;
    QString req = "SELECT * FROM shift_timetable";
    PGresult* res = NULL;

    try {
        res = select_from_conf(req.toStdString());

        int rec_count = PQntuples(res);

        if( res == nullptr || !rec_count) return false;

        for (int i = 0; i < rec_count; i++) {   //успешное обращение, есть записи
            shift_timetable tmp_table;
            QString tmp;
            tmp = PQgetvalue(res, i, 0);
            tmp_table.uid = tmp.toInt();
            tmp_table.name = PQgetvalue(res, i, 1);
            tmp_table.time_open = PQgetvalue(res, i, 2);
            m_timetable_values.push_back(tmp_table);
        }
        PQclear(res);

        for (auto& a : m_timetable_values) {
            std::string date_open = a.time_open.toStdString();
            replace_date_to_nowaday(a.time_open);                 // перед отправкой скорректируем дату открытия смены
            QDateTime tt = QDateTime::fromString(a.time_open, "yyyy-MM-dd hh:mm:ss");
            uint64_t timeUTC = tt.toUTC().toMSecsSinceEpoch();

            req = QString("insert into shifts (name, time_open, time_added) values ('%1', '%2', '%3');").arg(a.name).arg(timeUTC).arg(nowaday);

            if (!add_shift_column(req)){    // обрыв связи с бд. запомним и добавим запись при восстановлении
                emit BdNoConnectionWriteToFile(req);
            }

        }
        if ( !m_timetable_values.empty() ) return true;
        else return false;

    }  catch (...) {
        return false;
    }


}

QTime DB_Connection::getStartTime(bool& isStartShiftingTableLogic) const
{
    QTime setting_time_start;

    if (m_connect_arch && m_connect_conf) {

        QString req = "SELECT * FROM service_settings";
        PGresult* res = NULL;

        res = select_from_conf(req.toStdString());

        int rec_count = PQntuples(res);
        //
        for (int i = 0; i < rec_count; i++) {
            if (res != nullptr && rec_count > 0) {

                QString tmp;
                tmp = PQgetvalue(res, i, 0);	

                //
                if (tmp == "Проверка смен") {       //наша настройка
                  
                    tmp = PQgetvalue(res, i, 1);	
                    int is_hh_mm_ss = tmp.count(QLatin1Char(':'));      // определяем формат времени... либо hh:mm либо hh:mm:ss либо 0
                  
                    if (is_hh_mm_ss == 1) {
                        setting_time_start = QTime::fromString(tmp, "hh:mm");
                        isStartShiftingTableLogic=true;
                        break;
                    }
                    else if (is_hh_mm_ss == 2) {
                        setting_time_start = QTime::fromString(tmp, "hh:mm:ss");
                        isStartShiftingTableLogic=true;
                        break;
                    }
                    else if(tmp == "0"){
                        isStartShiftingTableLogic=false;        //логику вообще не запускаем
                        break;
                    }

                }
                else {
                    isStartShiftingTableLogic=false;
                    continue;
                }
            }
        }

    }
    else {
        setting_time_start = QTime::fromString("06:00:00", "hh:mm:ss");   // при обрыве связи время по умолчанию...
    }
    return setting_time_start;
}

//упрощенная версия функции... вызываем для готового запроса после восстановления связи с БД
void DB_Connection::insert_into_charging_tableAfterReconnect(const QString& type, const QString& msg, const quint32 a_serial, const quint32 a_value,int month_index,const QString& time)
{
    QString req, table_name, tp;
    int id_app = 0;
    table_name = QString("charge_station_log_%1").arg(month_index);
    tp = type;

    // определим тип устройства
    if (type.contains("Светильник")) {
        make_select_conf(std::string("devices"), std::string("device_type"), id_app, tp, a_serial);
    }
    else if (type.contains("Метка")) {
        make_select_conf(std::string("mark"), std::string("type_mark"), id_app, tp, a_serial);
    }
    else if (type.contains("ExLUCH-CHRG05")) {
        make_select_conf(std::string("luch"), std::string("type_luch"), id_app, tp, a_serial);
    }


    req = QString("insert into charge_station_log_%1 (id_app, type, descr, status_code, time) values (%2, '%3', '%4', %5,'%6');").arg(month_index).arg(id_app).arg(tp).arg(msg).arg(a_value).arg(time);

    //вдруг опять связи нет... добавим позже
     if ( !add_ChargingFailure_column(req, month_index))
     {
         req = QString("'%1''%2''%3''%4''%5''%6'").arg(type).arg(msg).arg(a_serial).arg(a_value).arg(month_index).arg(time);
         emit BdNoConnectionWriteToFile(req);
     }
}

// распарсили txt , получили вектор запросов и обработаем его
void DB_Connection::addReqsAfterReconnect() const
{
    if(!reqs.empty()){
        try{
            QStringList list;
            QString type, msg, time, tmp;
            quint32 a_serial, a_value;
            int month_index;

            for (const auto& a : reqs){

                list = a.split(QRegExp("'"), QString::SkipEmptyParts);          //для обработки ошибок с ЗС

                if(list.size() ==6)             //обрабатывает ошибки с ЗС
                {
                    type = list.at(0);
                    msg = list.at(1);
                    a_serial = list.at(2).toUInt();
                    a_value = list.at(3).toUInt();
                    month_index = list.at(4).toInt();
                    time = list.at(5);
                    insert_into_charging_tableAfterReconnect(type,msg,a_serial,a_value,month_index, time);
                }
                else if (a.contains("shifts"))                  //обрабатывает не попавшие смены
                {
                    if (!add_shift_column(a)){
                        emit BdNoConnectionWriteToFile(a);
                    }
                }
            }
            reqs.clear();
        }
        catch (...)
        {

        }
    }
}
