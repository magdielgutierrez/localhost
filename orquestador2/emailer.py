# -*- coding: utf-8 -*-
"""
Created on Wed Feb  7 15:34:36 2024

@author: RLARIOS
"""

import os
import platform
import filetype

# Linux
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from pathlib import Path
import smtplib


arch = ['br', 'rpm', 'dcm', 'epub', 'zip', 'tar', 'rar', 'gz', 'bz2', '7z', 'xz', 'pdf', 'exe', 'swf', 'rtf', 'eot', 'ps', 'sqlite', 'nes', 'crx', 'cab', 'deb', 'ar', 'z', 'lzo', 'lz', 'lz4', 'zstd']
docs = ['doc' , 'docx' , 'odt' , 'xls' , 'xlsx' , 'ods' , 'ppt' , 'pptx' , 'odp' , 'csv' ]

EXT_DENIED = arch + docs

DOMAINS = ["bam" , "bancolombia" , "banistmo" , "bancoagricola" ] #, "wenia" , "nequi"]

class Emailer:
    def __init__(self, logger , **kwargs):
        self.executions = 0
        self.log = logger
        
        self.max_executions = kwargs.get("mailExecs" , 3)
        self.host = kwargs.get("mailHost",  "exchange.bancolombia.corp")
        self.port = kwargs.get("mailPort" , 25)
        self.maxReceiver = kwargs.get("mailReceiverMax" , 10)
        self.__domains = kwargs.get("mailDomains" , DOMAINS)
        self.__denied = kwargs.get("mailDenied", EXT_DENIED)
        
        
    def __checkDomain(self , correo):
        campos = correo.split("@")
        if len(campos) > 1:
            domain = campos[1].lower().strip()
            check = False
            for d in self.__domains:
                if d in domain:
                    check = True
                
            if not check:
                raise ValueError("El correo '{0}' no hace parte de los dominios permitidos: {1}".format(correo,self.__domains) )
        
        
    def __validateParams(self , sender, receivers, subject, message , files = None):
        if type(sender) != str:
            raise ValueError("La variable sender del correo debe ser de tipo string")
        
        elif sender.strip() == "":
            raise ValueError("La variable sender del correo no debe ser vacía")
            
        else:
            self.__checkDomain(sender)
            
        
        if type(receivers) != list:
            raise ValueError("La variable receivers del correo debe ser de tipo list")
        else:
            if len(receivers) > self.maxReceiver:
                raise ValueError("El correo no se puede enviar a más de {0} direcciones.  Direcciones enviadas {1}".format(self.maxReceiver,len(receivers)))
                
            for idx , r in enumerate(receivers):
                if type(r) != str:
                    raise ValueError("El receptor en indice {0} debe ser de tipo string".format(idx))
                elif r.strip() == "":
                    raise ValueError("El receptor en indice {0} No debe estar vacío".format(idx))
                else:
                    self.__checkDomain( r )
                    
        if type(subject) != str:
            raise ValueError("La variable subject del correo debe ser de tipo string")
        
        elif subject.strip() == "":
            raise ValueError("La variable subject del correo no debe ser vacía")
            
        if type(message) != str:
            raise ValueError("La variable message del correo debe ser de tipo string")
        
        elif message.strip() == "":
            raise ValueError("La variable message del correo no debe ser vacía")
            
        if files is not None:    
            if type(files) == list:
                for idx , file in enumerate(files):
                    if type(file) != str:
                        raise ValueError("Elemento en el índice {0} de la lista de adjuntos debe ser de tipo str.  Tipo actual: {1}".format(idx,type(file)))
                    else:
                        if not os.path.exists(file):
                            raise ValueError("Elemento en el índice {0} de la lista de adjuntos no existe. Archivo: {1}".format(idx , file))
                        
                           
            else:
                raise ValueError("La variable 'files' del correo debe ser de tipo Lista o Nula")
                
    
    def __getExtension(self , filePath):
        filename, file_extension = os.path.splitext( filePath )
        return file_extension.replace(".","").strip().lower()
    
    
    def __validateFile(self,path):
        absol = os.path.abspath(path)
        kind = filetype.guess(absol)
        
        if kind is not None:
            exte = kind.extension.strip().lower()
            if exte in self.__denied:
                raise ValueError("No puede enviar adjuntos de tipo '{0}', Archivo: {1}".format(exte,absol))
                
        else:
            exte = self.__getExtension(absol)
            if exte in self.__denied:
                 raise ValueError("No puede enviar adjuntos de tipo '{0}', Archivo: {1}".format(exte,absol))
                 
        
     
        
        
    def send_email(self, sender, receivers, subject, message , files = None):
        
        self.__validateParams(sender, receivers, subject, message , files)
        
        attachment = True if files is not None else False

        opSys = platform.system()
        
        try:
            
            self.executions += 1
            if self.executions <= self.max_executions:
                if opSys.lower().strip() == "linux":
                    self.__send_email_linux( sender, receivers, subject, message, attachment , files )
                else:
                    self.__send_email_other( sender, receivers, subject, message, attachment , files )
                    
                
                self.log.info("Correo enviado satisfactoriamente")
            
            else:
                self.log.info("WARNING:  EL correo no puede ser enviado, ha superado el número máximo de correos a enviar en el proceso")
                

                
        except Exception as e:
            self.log.info("WARNING:  No se pudo enviar el correo")
            self.log.info("WARNING:  Error encontrado: {0}".format(e))
            

        
    def __send_email_linux(self, sender, receivers, subject, message, attachment = False, files = []) :
        """
            Envía un correo electrónico a una lista de correo con un mensaje definido y permite adjuntar archivos.

            Parameters
            ----------
            config : dict -> Diccionario con las variables requeridas ('host', 'port')
            sender: string -> Correo remitente del correo electrónico.
            receivers: list -> Listado con los correos receptores del correo electrónico.
            subject: string -> Asunto del correo electrónico.
            message: string -> Mensaje con la información que desea transmitir.
            attachment: bool -> Variable para indicar si se desean adjuntar o no archivos.
            files: list -> Listado con las rutas de ubicación de los archivos a adjuntar.

        """

        msg = MIMEMultipart() 
        msg['From'] = sender
        msg['To'] = COMMASPACE.join(receivers) 
        msg['Date'] = formatdate(localtime=True) 
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'html')) 
        
        if attachment:
            for path in files:
                self.__validateFile(path)
                part = MIMEBase('application', "octet-stream") 
                with open(path, 'rb') as file: 
                    part.set_payload(file.read()) 
                encoders.encode_base64(part) 
                part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(Path(path).name)) 
                msg.attach(part) 
        
        smtp = smtplib.SMTP()
        smtp.connect(self.host, self.port)
        smtp.ehlo(self.host)
        smtp.sendmail(sender, receivers, msg.as_string()) 
        smtp.quit()

    
            
    def __send_email_other(self, sender, receivers, subject, message, attachment = False, files = []):
    #(destinatario, asunto , mensaje , adjuntos = None , remitente = None):
        """Codigo para enviar mensajes por outlook.
        - sender es el usuario al cual se envía el mensaje (correo)
        - subject:  Asunto del correo
        - message:  Mensaje a enviar en el correo.  Se puede enviar en formato html
        - attachment:  Mensaje a enviar en el correo.  Se puede enviar en formato html
        - files:  lista con las rutas de los archivos a enviar
        """
        
        import win32com.client as win32

        def accountChg( o ):
            for oacc in o.Session.Accounts:
                if oacc.SmtpAddress.lower() == sender.lower():
                    return oacc
            return None
        
        def manageAttachments(mail):
            if attachment == False:
                return mail
            
            for idx , file in enumerate(files):
                absol = file if os.path.isabs(file) else os.path.abspath(file)
                self.__validateFile(absol)
                mail.Attachments.Add(absol)

            return mail


        otraCuenta = None  
        outlook = win32.Dispatch( 'outlook.application')
        msg = outlook.CreateItem(0)
        
        if sender:
            otraCuenta = accountChg( outlook)
    
        if otraCuenta:
            msg._oleobj_.Invoke(*(64209, 0, 8, 0, otraCuenta))
      
        msg.Subject = subject # Texto
        msg.To = ";".join(receivers) # correo
    
        msg.HTMLBody = message
        msg = manageAttachments( msg )
        
        msg.Send()  
                
        
    