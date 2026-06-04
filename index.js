const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
require('dotenv').config();

const JWT_SECRET = process.env.JWT_SECRET || 'bcs_turismo_secret_2026';
const NODO_NOMBRE = 'sebas';
const NODO_URL = 'https://bcs-backend-guadalupe-production-3f83.up.railway.app';

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  host:     process.env.DB_HOST,
  port:     process.env.DB_PORT,
  database: process.env.DB_NAME,
  user:     process.env.DB_USER,
  password: process.env.DB_PASSWORD,
});

const NODOS_REPLICAS = [
  'https://bcs-backend-guadalupe-production.up.railway.app',
  'https://bcs-backend-juan-production.up.railway.app',
  'https://bcs-backend-gama-production.up.railway.app',
  'https://bcs-backend-adan-production.up.railway.app',
];

// Guardar en log de replicación
async function guardarLog(tabla, operacion, datos, nodoOrigen = null) {
  try {
    const origen = nodoOrigen || NODO_NOMBRE;
    const result = await pool.query(
      `INSERT INTO log_replicacion (tabla, operacion, datos, nodo_origen, creado_en)
       VALUES ($1, $2, $3, $4, NOW()) RETURNING id`,
      [tabla, operacion, JSON.stringify(datos), origen]
    );
    console.log(`📝 Log guardado [${origen}]: ${tabla} - ${operacion} (ID: ${result.rows[0].id})`);
    return result.rows[0].id;
  } catch (err) {
    console.error('❌ Error guardando log:', err.message);
    return null;
  }
}

// Replicar a otros nodos
async function replicarANodos(endpoint, datos, tabla, operacion) {
  console.log(`📤 Replicando ${tabla} - ${operacion} a ${NODOS_REPLICAS.length} nodos...`);
  
  for (const nodo of NODOS_REPLICAS) {
    try {
      const response = await fetch(`${nodo.url}${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Replicacion': 'true',
          'X-Nodo-Origen': NODO_NOMBRE
        },
        body: JSON.stringify(datos),
        signal: AbortSignal.timeout(5000)
      });

      if (response.ok) {
        console.log(`✅ Replicado a ${nodo.nombre} (${nodo.url})`);
      } else {
        console.log(`⚠️ Error en ${nodo.nombre}: ${response.status}`);
      }
    } catch (err) {
      console.error(`❌ Error replicando a ${nodo.nombre}:`, err.message);
    }
  }
}

// Obtener el último log de un nodo específico
async function obtenerUltimoLogDeNodo(nodoNombre) {
  const result = await pool.query(
    `SELECT MAX(creado_en) as ultimo 
     FROM log_replicacion 
     WHERE nodo_origen = $1`,
    [nodoNombre]
  );
  return result.rows[0].ultimo || new Date('2024-01-01');
}

// Sincronización completa con todos los nodos
async function sincronizarConTodosLosNodos() {
  console.log('\n🔄 INICIANDO SINCRONIZACIÓN COMPLETA...');
  
  for (const nodo of NODOS_REPLICAS) {
    await sincronizarConNodo(nodo);
  }
  
  console.log('✅ SINCRONIZACIÓN COMPLETADA\n');
}

// Sincronizar con un nodo específico
async function sincronizarConNodo(nodo) {
  try {
    console.log(`\n📡 Sincronizando con ${nodo.nombre}...`);
    
    // Obtener la fecha del último log que tenemos de este nodo
    const ultimoLog = await obtenerUltimoLogDeNodo(nodo.nombre);
    const desde = ultimoLog.toISOString();
    
    console.log(`   Último log de ${nodo.nombre}: ${desde}`);
    
    // Solicitar logs nuevos al nodo
    const response = await fetch(`${nodo.url}/api/replicacion/sync?desde=${encodeURIComponent(desde)}&nodo=${NODO_NOMBRE}`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
      signal: AbortSignal.timeout(10000)
    });
    
    if (!response.ok) {
      console.log(`   ❌ Error ${response.status} al sincronizar con ${nodo.nombre}`);
      return;
    }
    
    const logs = await response.json();
    
    if (logs.length === 0) {
      console.log(`   ✅ No hay logs nuevos de ${nodo.nombre}`);
      return;
    }
    
    console.log(`   📥 Recibidos ${logs.length} logs nuevos de ${nodo.nombre}`);
    
    // Aplicar cada log
    for (const log of logs) {
      await aplicarLogRecibido(log);
    }
    
    console.log(`   ✅ Sincronización con ${nodo.nombre} completada`);
    
  } catch (err) {
    console.error(`   ❌ Error sincronizando con ${nodo.nombre}:`, err.message);
  }
}

// Aplicar un log recibido de otro nodo
async function aplicarLogRecibido(log) {
  try {
    // Verificar si ya tenemos este log (por el ID o por contenido)
    const existe = await pool.query(
      'SELECT id FROM log_replicacion WHERE id = $1 OR (tabla = $2 AND datos = $3 AND nodo_origen = $4)',
      [log.id, log.tabla, log.datos, log.nodo_origen]
    );
    
    if (existe.rows.length > 0) {
      console.log(`   ⏭️ Log ${log.id} ya existe, omitiendo`);
      return;
    }
    
    console.log(`   🔄 Aplicando log ${log.id}: ${log.tabla} - ${log.operacion} desde ${log.nodo_origen}`);
    
    // Aplicar el cambio según la tabla
    if (log.tabla === 'resenas') {
      const datos = typeof log.datos === 'string' ? JSON.parse(log.datos) : log.datos;
      
      // Verificar si la reseña ya existe
      const existeResena = await pool.query(
        'SELECT id FROM resenas WHERE id = $1',
        [datos.id]
      );
      
      if (existeResena.rows.length === 0) {
        await pool.query(
          `INSERT INTO resenas (lugar_id, actividad_id, usuario_id, titulo, comentario, estrellas, creado_en)
           VALUES ($1, $2, $3, $4, $5, $6, NOW())`,
          [datos.lugar_id, datos.actividad_id, datos.usuario_id, datos.titulo, datos.comentario, datos.estrellas]
        );
        console.log(`   ✅ Reseña insertada`);
      }
      
    } else if (log.tabla === 'usuarios') {
      const datos = typeof log.datos === 'string' ? JSON.parse(log.datos) : log.datos;
      
      const existeUsuario = await pool.query(
        'SELECT id FROM usuarios WHERE correo = $1',
        [datos.correo]
      );
      
      if (existeUsuario.rows.length === 0) {
        await pool.query(
          `INSERT INTO usuarios (nombre_usuario, correo, contrasena_hash, creado_en)
           VALUES ($1, $2, $3, NOW())`,
          [datos.nombre_usuario, datos.correo, datos.contrasena_hash]
        );
        console.log(`   ✅ Usuario ${datos.nombre_usuario} insertado`);
      }
    }
    
    // Guardar el log localmente también
    await guardarLog(log.tabla, log.operacion, log.datos, log.nodo_origen);
    
  } catch (err) {
    console.error(`   ❌ Error aplicando log:`, err.message);
  }
}

// Sincronización periódica (cada 5 minutos)
function iniciarSincronizacionPeriodica() {
  setInterval(async () => {
    console.log('\n⏰ Sincronización periódica iniciada...');
    await sincronizarConTodosLosNodos();
  }, 5 * 60 * 1000); // 5 minutos
}

// ✅ Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', nodo: NODO_NOMBRE });
});

// ✅ Endpoint de sincronización - devuelve logs desde un timestamp
app.get('/api/replicacion/sync', async (req, res) => {
  try {
    const { desde, nodo } = req.query;
    const desdeDate = desde ? new Date(desde) : new Date('2024-01-01');
    
    console.log(`📥 Solicitud de sync desde ${nodo} (desde: ${desdeDate.toISOString()})`);
    
    const result = await pool.query(
      `SELECT * FROM log_replicacion 
       WHERE creado_en > $1 AND nodo_origen != $2
       ORDER BY creado_en ASC`,
      [desdeDate, nodo]
    );
    
    console.log(`📤 Enviando ${result.rows.length} logs a ${nodo}`);
    res.json(result.rows);
    
  } catch (err) {
    console.error('Error en sync:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ✅ Endpoint para ver todos los logs (debug)
app.get('/api/replicacion/logs', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM log_replicacion ORDER BY creado_en DESC LIMIT 100'
    );
    res.json({
      total: result.rows.length,
      nodo_actual: NODO_NOMBRE,
      logs: result.rows
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ Endpoint para forzar sincronización manual
app.post('/api/replicacion/sincronizar', async (req, res) => {
  try {
    await sincronizarConTodosLosNodos();
    res.json({ message: 'Sincronización completada', nodo: NODO_NOMBRE });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ REGIONES
app.get('/api/regiones', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM regiones');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ CATEGORIAS
app.get('/api/categorias', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM categorias');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ LUGARES
app.get('/api/lugares', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT l.*, r.nombre as region, c.nombre as categoria
      FROM lugares l
      JOIN regiones r ON l.region_id = r.id
      LEFT JOIN categorias c ON l.categoria_id = c.id
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/lugares/region/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT l.*, c.nombre as categoria
      FROM lugares l
      LEFT JOIN categorias c ON l.categoria_id = c.id
      WHERE l.region_id = $1
    `, [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/lugares/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT l.*, r.nombre as region, c.nombre as categoria
      FROM lugares l
      JOIN regiones r ON l.region_id = r.id
      LEFT JOIN categorias c ON l.categoria_id = c.id
      WHERE l.id = $1
    `, [req.params.id]);
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/lugares', async (req, res) => {
  try {
    const { region_id, nombre, descripcion, latitud, longitud, categoria_id, imagen_url } = req.body;
    const result = await pool.query(
      `INSERT INTO lugares (region_id, nombre, descripcion, latitud, longitud, categoria_id, imagen_url)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [region_id, nombre, descripcion, latitud, longitud, categoria_id, imagen_url]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ ACTIVIDADES
app.get('/api/actividades/region/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT a.*, c.nombre as categoria
      FROM actividades a
      LEFT JOIN categorias c ON a.categoria_id = c.id
      WHERE a.region_id = $1
    `, [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/actividades/lugar/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT a.*, c.nombre as categoria
      FROM actividades a
      LEFT JOIN categorias c ON a.categoria_id = c.id
      WHERE a.lugar_id = $1
    `, [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/actividades', async (req, res) => {
  try {
    const { region_id, lugar_id, nombre, descripcion, duracion, dificultad, categoria_id, imagen_url } = req.body;
    const result = await pool.query(
      `INSERT INTO actividades (region_id, lugar_id, nombre, descripcion, duracion, dificultad, categoria_id, imagen_url)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [region_id, lugar_id, nombre, descripcion, duracion, dificultad, categoria_id, imagen_url]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ FOTOS
app.get('/api/fotos/lugar/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM fotos WHERE lugar_id = $1', [req.params.id]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/fotos', async (req, res) => {
  try {
    const { lugar_id, url, descripcion } = req.body;
    const result = await pool.query(
      `INSERT INTO fotos (lugar_id, url, descripcion) VALUES ($1,$2,$3) RETURNING *`,
      [lugar_id, url, descripcion]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ TIPS ECOLOGÍA
app.get('/api/tips/lugar/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM tips_ecologia WHERE lugar_id = $1', [req.params.id]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/tips', async (req, res) => {
  try {
    const { lugar_id, tip } = req.body;
    const result = await pool.query(
      `INSERT INTO tips_ecologia (lugar_id, tip) VALUES ($1,$2) RETURNING *`,
      [lugar_id, tip]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ RECOMENDACIONES
app.get('/api/recomendaciones/lugar/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM recomendaciones WHERE lugar_id = $1', [req.params.id]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/recomendaciones', async (req, res) => {
  try {
    const { lugar_id, recomendacion } = req.body;
    const result = await pool.query(
      `INSERT INTO recomendaciones (lugar_id, recomendacion) VALUES ($1,$2) RETURNING *`,
      [lugar_id, recomendacion]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ LOCALES DE COMIDA
app.get('/api/comida/lugar/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM locales_comida WHERE lugar_id = $1', [req.params.id]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/comida', async (req, res) => {
  try {
    const { lugar_id, nombre, tipo_cocina, direccion, telefono, precio_promedio } = req.body;
    const result = await pool.query(
      `INSERT INTO locales_comida (lugar_id, nombre, tipo_cocina, direccion, telefono, precio_promedio)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [lugar_id, nombre, tipo_cocina, direccion, telefono, precio_promedio]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ CÓDIGOS DE CONDUCTA
app.get('/api/conducta/lugar/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM codigos_conducta WHERE lugar_id = $1', [req.params.id]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/conducta', async (req, res) => {
  try {
    const { lugar_id, regla } = req.body;
    const result = await pool.query(
      `INSERT INTO codigos_conducta (lugar_id, regla) VALUES ($1,$2) RETURNING *`,
      [lugar_id, regla]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ RESEÑAS - UN SOLO ENDPOINT
app.post('/api/resenas', async (req, res) => {
  try {
    const esReplicacion = req.headers['x-replicacion'] === 'true';
    const nodoOrigen = req.headers['x-nodo-origen'];
    const { lugar_id, actividad_id, usuario_id, titulo, comentario, estrellas } = req.body;
    
    // Si es replicación, solo insertar y guardar log
    if (esReplicacion) {
      const existe = await pool.query(
        'SELECT id FROM resenas WHERE lugar_id = $1 AND usuario_id = $2 AND titulo = $3',
        [lugar_id, usuario_id, titulo]
      );
      
      if (existe.rows.length === 0) {
        await pool.query(
          `INSERT INTO resenas (lugar_id, actividad_id, usuario_id, titulo, comentario, estrellas, creado_en)
           VALUES ($1, $2, $3, $4, $5, $6, NOW())`,
          [lugar_id, actividad_id ?? null, usuario_id, titulo, comentario, estrellas]
        );
        console.log(`✅ Reseña replicada para lugar ${lugar_id}`);
        
        // Guardar en log local también
        await guardarLog('resenas', 'INSERT', { lugar_id, actividad_id, usuario_id, titulo, comentario, estrellas }, nodoOrigen);
      }
      return res.json({ success: true });
    }
    
    // Si NO es replicación, crear nueva reseña
    const result = await pool.query(
      `INSERT INTO resenas (lugar_id, actividad_id, usuario_id, titulo, comentario, estrellas, creado_en)
       VALUES ($1, $2, $3, $4, $5, $6, NOW()) RETURNING *`,
      [lugar_id, actividad_id ?? null, usuario_id, titulo, comentario, estrellas]
    );
    
    res.json(result.rows[0]);
    
    // Guardar log y replicar
    const datos = { lugar_id, actividad_id, usuario_id, titulo, comentario, estrellas };
    await guardarLog('resenas', 'INSERT', datos);
    await replicarANodos('/api/resenas', datos, 'resenas', 'INSERT');
    
  } catch (err) {
    console.error('Error en reseñas:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/resenas/region/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT r.*, u.nombre_usuario, u.foto_url as usuario_foto,
             l.nombre as lugar_nombre, a.nombre as actividad_nombre
      FROM resenas r
      LEFT JOIN usuarios u ON r.usuario_id = u.id
      LEFT JOIN lugares l ON r.lugar_id = l.id
      LEFT JOIN actividades a ON r.actividad_id = a.id
      WHERE r.lugar_id IN (SELECT id FROM lugares WHERE region_id = $1)
      OR r.actividad_id IN (SELECT id FROM actividades WHERE region_id = $1)
      ORDER BY r.creado_en DESC
    `, [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/resenas/lugar/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT r.*, u.nombre_usuario, u.foto_url as usuario_foto
      FROM resenas r
      LEFT JOIN usuarios u ON r.usuario_id = u.id
      WHERE r.lugar_id = $1
      ORDER BY r.creado_en DESC
    `, [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/resenas/actividad/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT r.*, u.nombre_usuario, u.foto_url as usuario_foto
      FROM resenas r
      LEFT JOIN usuarios u ON r.usuario_id = u.id
      WHERE r.actividad_id = $1
      ORDER BY r.creado_en DESC
    `, [req.params.id]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ GUIA POR REGIÓN
app.get('/api/guia/region/:id', async (req, res) => {
  try {
    const [tips, recomendaciones, conducta] = await Promise.all([
      pool.query(`
        SELECT t.* FROM tips_ecologia t
        JOIN lugares l ON t.lugar_id = l.id
        WHERE l.region_id = $1
      `, [req.params.id]),
      pool.query(`
        SELECT r.* FROM recomendaciones r
        JOIN lugares l ON r.lugar_id = l.id
        WHERE l.region_id = $1
      `, [req.params.id]),
      pool.query(`
        SELECT c.* FROM codigos_conducta c
        JOIN lugares l ON c.lugar_id = l.id
        WHERE l.region_id = $1
      `, [req.params.id]),
    ]);
    res.json({
      tips: tips.rows,
      recomendaciones: recomendaciones.rows,
      conducta: conducta.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ AUTENTICACIÓN - UN SOLO ENDPOINT
app.post('/api/auth/registro', async (req, res) => {
  try {
    const esReplicacion = req.headers['x-replicacion'] === 'true';
    const nodoOrigen = req.headers['x-nodo-origen'];
    const { nombre_usuario, correo, contrasena } = req.body;

    // Si es replicación, solo insertar
    if (esReplicacion) {
      const existe = await pool.query(
        'SELECT id FROM usuarios WHERE correo = $1 OR nombre_usuario = $2',
        [correo, nombre_usuario]
      );
      
      if (existe.rows.length === 0) {
        const hash = await bcrypt.hash(contrasena, 10);
        await pool.query(
          `INSERT INTO usuarios (nombre_usuario, correo, contrasena_hash, creado_en)
           VALUES ($1, $2, $3, NOW())`,
          [nombre_usuario, correo, hash]
        );
        console.log(`✅ Usuario replicado: ${nombre_usuario}`);
        
        // Guardar log de la replicación recibida
        await guardarLog('usuarios', 'INSERT', { nombre_usuario, correo, contrasena_hash: hash }, nodoOrigen);
      }
      return res.json({ success: true });
    }

    // Registro normal
    const existe = await pool.query(
      'SELECT id FROM usuarios WHERE correo = $1 OR nombre_usuario = $2',
      [correo, nombre_usuario]
    );
    
    if (existe.rows.length > 0) {
      return res.status(400).json({ error: 'El correo o nombre de usuario ya está en uso' });
    }

    const hash = await bcrypt.hash(contrasena, 10);
    const result = await pool.query(
      `INSERT INTO usuarios (nombre_usuario, correo, contrasena_hash, creado_en)
       VALUES ($1, $2, $3, NOW()) RETURNING id, nombre_usuario, correo, foto_url`,
      [nombre_usuario, correo, hash]
    );

    const usuario = result.rows[0];
    const token = jwt.sign({ id: usuario.id }, JWT_SECRET, { expiresIn: '30d' });
    
    res.json({ usuario, token });
    
    // Guardar log y replicar
    const datos = { nombre_usuario, correo, contrasena_hash: hash };
    await guardarLog('usuarios', 'INSERT', datos);
    await replicarANodos('/api/auth/registro', { nombre_usuario, correo, contrasena }, 'usuarios', 'INSERT');
    
  } catch (err) {
    console.error('Error en registro:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { correo, contrasena } = req.body;
    const result = await pool.query(
      'SELECT * FROM usuarios WHERE correo = $1', [correo]
    );
    if (result.rows.length === 0) {
      return res.status(401).json({ error: 'Correo o contraseña incorrectos' });
    }
    const usuario = result.rows[0];
    const valido = await bcrypt.compare(contrasena, usuario.contrasena_hash);
    if (!valido) {
      return res.status(401).json({ error: 'Correo o contraseña incorrectos' });
    }
    const token = jwt.sign({ id: usuario.id }, JWT_SECRET, { expiresIn: '30d' });
    res.json({
      usuario: {
        id: usuario.id,
        nombre_usuario: usuario.nombre_usuario,
        correo: usuario.correo,
        foto_url: usuario.foto_url,
      },
      token
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/auth/perfil', async (req, res) => {
  try {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'No autorizado' });
    const decoded = jwt.verify(token, JWT_SECRET);
    const result = await pool.query(
      'SELECT id, nombre_usuario, correo, foto_url, creado_en FROM usuarios WHERE id = $1',
      [decoded.id]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(401).json({ error: 'Token inválido' });
  }
});

app.put('/api/auth/perfil', async (req, res) => {
  try {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'No autorizado' });
    const decoded = jwt.verify(token, JWT_SECRET);
    const { nombre_usuario, correo, foto_url, contrasena_verificacion } = req.body;
    const usuarioActual = await pool.query(
      'SELECT * FROM usuarios WHERE id = $1', [decoded.id]
    );
    if (correo && correo !== usuarioActual.rows[0].correo) {
      if (!contrasena_verificacion) {
        return res.status(400).json({ error: 'Se requiere contraseña para cambiar el correo' });
      }
      const valido = await bcrypt.compare(contrasena_verificacion, usuarioActual.rows[0].contrasena_hash);
      if (!valido) {
        return res.status(401).json({ error: 'Contraseña incorrecta' });
      }
    }
    const result = await pool.query(
      `UPDATE usuarios SET 
        nombre_usuario = COALESCE($1, nombre_usuario),
        correo = COALESCE($2, correo),
        foto_url = COALESCE($3, foto_url),
        actualizado_en = NOW()
       WHERE id = $4 RETURNING id, nombre_usuario, correo, foto_url`,
      [nombre_usuario, correo, foto_url, decoded.id]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/auth/contrasena', async (req, res) => {
  try {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'No autorizado' });
    const decoded = jwt.verify(token, JWT_SECRET);
    const { contrasena_actual, contrasena_nueva } = req.body;
    const result = await pool.query(
      'SELECT contrasena_hash FROM usuarios WHERE id = $1', [decoded.id]
    );
    const valido = await bcrypt.compare(contrasena_actual, result.rows[0].contrasena_hash);
    if (!valido) return res.status(401).json({ error: 'Contraseña actual incorrecta' });
    const hash = await bcrypt.hash(contrasena_nueva, 10);
    await pool.query(
      'UPDATE usuarios SET contrasena_hash = $1 WHERE id = $2', [hash, decoded.id]
    );
    res.json({ mensaje: 'Contraseña actualizada correctamente' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ LOGIN CON GOOGLE
app.post('/api/auth/google', async (req, res) => {
  try {
    const { google_id, correo, nombre, foto_url } = req.body;
    let result = await pool.query(
      'SELECT * FROM usuarios WHERE google_id = $1', [google_id]
    );
    if (result.rows.length > 0) {
      const usuario = result.rows[0];
      const token = jwt.sign({ id: usuario.id }, JWT_SECRET, { expiresIn: '30d' });
      return res.json({
        usuario: {
          id: usuario.id,
          nombre_usuario: usuario.nombre_usuario,
          correo: usuario.correo,
          foto_url: usuario.foto_url,
        },
        token
      });
    }
    result = await pool.query('SELECT * FROM usuarios WHERE correo = $1', [correo]);
    if (result.rows.length > 0) {
      await pool.query(
        'UPDATE usuarios SET google_id = $1, foto_url = COALESCE(foto_url, $2) WHERE correo = $3',
        [google_id, foto_url, correo]
      );
      const usuario = result.rows[0];
      const token = jwt.sign({ id: usuario.id }, JWT_SECRET, { expiresIn: '30d' });
      return res.json({
        usuario: {
          id: usuario.id,
          nombre_usuario: usuario.nombre_usuario,
          correo: usuario.correo,
          foto_url: foto_url || usuario.foto_url,
        },
        token
      });
    }
    let nombreUsuario = nombre.toLowerCase().replace(/\s+/g, '_');
    const existeUsername = await pool.query(
      'SELECT id FROM usuarios WHERE nombre_usuario = $1', [nombreUsuario]
    );
    if (existeUsername.rows.length > 0) {
      nombreUsuario = `${nombreUsuario}_${Math.floor(Math.random() * 9000) + 1000}`;
    }
    const nuevoUsuario = await pool.query(
      `INSERT INTO usuarios (nombre_usuario, correo, google_id, foto_url, proveedor, creado_en)
       VALUES ($1, $2, $3, $4, 'google', NOW()) RETURNING id, nombre_usuario, correo, foto_url`,
      [nombreUsuario, correo, google_id, foto_url]
    );
    const token = jwt.sign({ id: nuevoUsuario.rows[0].id }, JWT_SECRET, { expiresIn: '30d' });
    res.json({ usuario: nuevoUsuario.rows[0], token });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ NODOS
app.get('/api/nodos', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM nodos');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ ESTADO DE REPLICACIÓN
app.get('/api/replicacion/estado', async (req, res) => {
  const estados = [];
  for (const nodo of NODOS_REPLICAS) {
    try {
      const response = await fetch(`${nodo.url}/health`, {
        signal: AbortSignal.timeout(3000)
      });
      const data = await response.json();
      estados.push({ nodo: nodo.nombre, url: nodo.url, estado: 'activo', info: data });
    } catch (err) {
      estados.push({ nodo: nodo.nombre, url: nodo.url, estado: 'caido', error: err.message });
    }
  }
  
  // Obtener estadísticas de logs
  const logStats = await pool.query(`
    SELECT nodo_origen, COUNT(*) as total_logs, MAX(creado_en) as ultimo_log
    FROM log_replicacion 
    GROUP BY nodo_origen
  `);
  
  res.json({
    nodo_actual: NODO_NOMBRE,
    url: NODO_URL,
    replicas: estados,
    logs_por_nodo: logStats.rows
  });
});

// Iniciar servidor
app.listen(process.env.PORT, async () => {
  console.log(`\n🚀 Nodo ${NODO_NOMBRE} corriendo en puerto ${process.env.PORT}`);
  console.log(`📋 URL: ${NODO_URL}\n`);
  
  // Esperar 5 segundos para que el servidor esté listo
  setTimeout(async () => {
    await sincronizarConTodosLosNodos();
    iniciarSincronizacionPeriodica();
    console.log('✅ Sistema de replicación inicializado\n');
  }, 5000);
});