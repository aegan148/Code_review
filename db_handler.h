#pragma once
#include "../db/DBCon.h"
#include "../db/DBConnection.h"
#include <QString>
#include <chrono>
#include <QVector>
#include <QSettings>
#include <QThread>
#include <QTime>
#include <QTimer>
#include <qobject.h>
#include <tchar.h>
#include <direct.h>
#include "atlconv.h" 
#include "connectionfailureobj.h"

const uint64_t hours24_ms = 86400000;


class DB_Connection : public QObject {
	Q_OBJECT

public:
    DB_Connection(QObject *parent = nullptr) : QObject(parent), m_srvDBdepmineConn(NULL), m_srvDBdepminearchConn(NULL), m_depmine(NULL), m_depminearch(NULL), m_connect_conf(false), m_connect_arch(false);
    virtual ~DB_Connection() { _thr1.quit();_thr1.wait();delete m_srvDBdepminearchConn; delete m_srvDBdepmineConn; delete m_depmine; delete m_depminearch; }
	
  bool set_connect();
  bool add_chargingFailure_table(const int& month_index) const;
  bool add_ChargingFailure_column(const QString& req, int& month_index) const;
	bool is_connect_conf() const { return m_connect_conf; }
	bool is_connect_arch() const { return m_connect_arch; }
  bool is_shift_table_empty(const QString& table_name) const;
  bool add_shift_column(const QString& req) const;
  bool is_charging_table_empty(const QString& table_name, int & maxCount) const;
  
  void reset_connect();
	void set_IP(const QString& ip);
	void set_Port(const QString& port);
	void set_db_arch_name(const QString& arch_name);
	void set_db_conf_name(const QString& conf);
	void set_user(const QString& username);
	void set_password(const QString& pswd);
  void make_select_conf(const std::string& table, const std::string& row, int& id_app, QString& type, const quint32 a_serial) const;
	void insert_into_charging_tableAfterReconnect(const QString& type, const QString& msg, const quint32 a_serial, const quint32 a_value,int month_index,const QString& time);
    
	PGresult* select_from_conf(const std::string& req) const;
	PGresult* select_from_arch(const std::string& req) const;    
	
	DBConnection* srvDBdepmineConnOBJ() const { return m_srvDBdepmineConn; }
	DBConnection* srvDBdepminearchConnOBJ() const { return m_srvDBdepminearchConn; }
    
signals:
    void BdNoConnection();
    void BdConnectionOk();
    void BdNoConnectionWriteToFile(const QString& req);
public slots:
    void insert_into_charging_table(const QString& type, const QString& msg, const quint32 a_serial, const quint32 a_value) const;
    bool insert_into_shifting_table(const QString& nowaday) const;
    QTime getStartTime(bool& isStartShiftingTableLogic) const;
    void addReqsAfterReconnect() const;

private:
	DBConnection* m_srvDBdepmineConn;
	DBConnection* m_srvDBdepminearchConn;
	DBCon* m_depmine;
	DBCon* m_depminearch;
  QThread _thr1;
  QMutex m_ReqsStorageMutex;
  connectionFailureObj* m_reconnectObj;
  QVector<QString> reqs;
  QTimer* m_checkFileTimer;
	QString m_ip;
	QString m_port;
	QString m_db_arch_name;
	QString m_db_conf_name;
	QString m_username;
	QString m_pswd;
	bool m_connect_conf;
  bool m_connect_arch;
};
