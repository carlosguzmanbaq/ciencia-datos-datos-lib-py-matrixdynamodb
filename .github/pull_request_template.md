## 📋 Descripción

<!-- Describe brevemente los cambios realizados -->

## 🎯 Tipo de Cambio

- [ ] 🐛 Bug fix (corrección de error)
- [ ] ✨ Nueva funcionalidad (feature)
- [ ] 💥 Breaking change (cambio que rompe compatibilidad)
- [ ] 📝 Documentación
- [ ] ♻️ Refactorización
- [ ] ⚡ Mejora de performance
- [ ] ✅ Tests
- [ ] 🔧 Configuración

## 🔗 Issue Relacionado

<!-- Enlaza el issue o ticket relacionado -->
Closes #(issue)

## 🧪 Componentes Afectados

- [ ] Core Configuration (`JobConfig`, `JobParameters`, `Constants`)
- [ ] Extract (`Catalog`, `Sources`)
- [ ] Transform (`flatten_dynamodb_struct`, `curated_transformations`)
- [ ] Load (`Hudi`, `metadata`)
- [ ] Event Status (CDC tracking)
- [ ] Models
- [ ] Tests
- [ ] Documentación
- [ ] Ejemplos

## ✅ Checklist

### Código
- [ ] El código sigue las convenciones del proyecto
- [ ] Los nombres de variables/funciones son descriptivos
- [ ] Se agregaron docstrings en inglés para nuevas clases/métodos
- [ ] Se manejaron casos edge y errores apropiadamente
- [ ] No hay código comentado o debug prints

### Tests
- [ ] Se agregaron tests para los cambios
- [ ] Todos los tests pasan localmente (`pytest tests/ -v`)
- [ ] Cobertura de código >= 80% en nuevas funciones
- [ ] Se probó con datos FULL e INCREMENTAL (si aplica)

### Documentación
- [ ] Se actualizó el README si es necesario
- [ ] Se actualizó `project-context.md` si hay cambios arquitectónicos
- [ ] Se agregaron ejemplos de uso si es una nueva feature
- [ ] Se documentaron breaking changes

### Validación
- [ ] Se probó con múltiples primary keys
- [ ] Se validó el manejo de schemas (FULL con schema definido, INC con inferencia)
- [ ] Se verificó el flatten con diferentes profundidades
- [ ] Se probó con event_status=True/False (si aplica)

## 🧪 Cómo Probar

<!-- Describe los pasos para probar los cambios -->

```python
# Ejemplo de código para probar
```

## 📸 Screenshots / Logs

<!-- Si aplica, agrega capturas o logs relevantes -->

## 📚 Notas Adicionales

<!-- Información adicional que los revisores deban conocer -->

## 🔍 Impacto

<!-- Describe el impacto de estos cambios -->
- **Performance**: 
- **Compatibilidad**: 
- **Dependencias**: 

---

**Reviewer Checklist:**
- [ ] El código es claro y mantenible
- [ ] Los tests cubren casos importantes
- [ ] La documentación es suficiente
- [ ] No hay problemas de seguridad evidentes
- [ ] El cambio está alineado con la arquitectura del proyecto
