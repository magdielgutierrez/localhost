SELECT 'cartera' Nombre_Esquema, 'carte1' Nombre_Tabla, Count(*) Conteo from cobis.carte1 join 
SELECT 'cartera' Nombre_Esquema, 'cart2' Nombre_Tabla, Count(*) Conteo from cobis.cart2;


DESCRIBE cartera.carte1;
DESCRIBE cartera.cart2;