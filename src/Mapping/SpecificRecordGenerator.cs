using Avro;
using Avro.Specific;
using Kafka.Ksql.Linq.Configuration;
using Kafka.Ksql.Linq.Core.Attributes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace Kafka.Ksql.Linq.Mapping;

/// <summary>
/// Generate ISpecificRecord implementations from POCO types at runtime.
/// </summary>
internal static class SpecificRecordGenerator
{
    private static readonly ConcurrentDictionary<string, Lazy<Type>> _cache = new();
    private static readonly ModuleBuilder _moduleBuilder;

    static SpecificRecordGenerator()
    {
        var asmName = new AssemblyName("KafkaKsqlSpecificRecords");
        var asmBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        _moduleBuilder = asmBuilder.DefineDynamicModule("Main");
    }

    /// <summary>
    /// Generate (or retrieve from cache) the ISpecificRecord implementation for the given POCO type.
    /// </summary>
    public static Type Generate(Type pocoType)
    {
        var cacheKey = (pocoType.FullName ?? pocoType.Name).Replace('+', '.');
        var lazy = _cache.GetOrAdd(cacheKey, _ => new Lazy<Type>(() =>
        {
            var schemaJson = GenerateAvroSchema(pocoType);
            return GenerateSpecificRecordType(pocoType, schemaJson, cacheKey);
        }, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication));
        return lazy.Value;
    }

    private static string GenerateAvroSchema(Type pocoType)
    {
        var sb = new StringBuilder();
        sb.AppendLine("{");
        sb.AppendLine("  \"type\": \"record\",");
        sb.AppendLine($"  \"name\": \"{pocoType.Name}Avro\",");
        sb.AppendLine($"  \"namespace\": \"{pocoType.Namespace}\",");
        var props = pocoType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        if (props.Length == 0)
        {
            sb.AppendLine("  \"fields\": []");
        }
        else
        {
            sb.AppendLine("  \"fields\": [");
            for (int i = 0; i < props.Length; i++)
            {
                var p = props[i];
                var decAttr = p.GetCustomAttribute<KsqlDecimalAttribute>();
                var avroType = MapToAvroType(p.PropertyType, decAttr, p);

                sb.Append($"    {{ \"name\": \"{p.Name}\", \"type\": {avroType}");

                if (p.PropertyType == typeof(string))
                    sb.Append(", \"default\": \"\"");
                else if (p.PropertyType == typeof(int) || p.PropertyType == typeof(long) ||
                         p.PropertyType == typeof(float) || p.PropertyType == typeof(double))
                    sb.Append(", \"default\": 0");
                else if (p.PropertyType == typeof(bool))
                    sb.Append(", \"default\": false");
                else if (p.PropertyType == typeof(byte[]))
                    sb.Append(", \"default\": \"\"");
                else if (p.PropertyType == typeof(DateTime))
                    sb.Append(", \"default\": 0");
                else if (p.PropertyType == typeof(Guid))
                    sb.Append(", \"default\": \"00000000-0000-0000-0000-000000000000\"");
                else if (p.PropertyType.IsGenericType &&
                         p.PropertyType.GetGenericTypeDefinition() == typeof(Dictionary<,>) &&
                         p.PropertyType.GetGenericArguments()[0] == typeof(string) &&
                         p.PropertyType.GetGenericArguments()[1] == typeof(string))
                    sb.Append(", \"default\": {}");
                else if (Nullable.GetUnderlyingType(p.PropertyType) != null)
                    sb.Append(", \"default\": null");

                sb.Append(" }");
                if (i < props.Length - 1) sb.Append(',');
                sb.AppendLine();
            }
            sb.AppendLine("  ]");
        }
        sb.Append('}');
        return sb.ToString();
    }

    private static string MapToAvroType(Type t, KsqlDecimalAttribute? decAttr, System.Reflection.PropertyInfo? prop)
    {
        // Handle Nullable<T> as union ["null", <T>]
        var underlying = Nullable.GetUnderlyingType(t);
        if (underlying != null)
        {
            var inner = MapToAvroType(underlying, decAttr, prop);
            return $"[ \"null\", {inner} ]";
        }
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var args = t.GetGenericArguments();
            if (args[0] == typeof(string) && args[1] == typeof(string))
                return "{ \"type\": \"map\", \"values\": \"string\" }";
            throw new NotSupportedException("Only Dictionary<string,string> is supported.");
        }
        if (t == typeof(int)) return "\"int\"";
        if (t == typeof(long)) return "\"long\"";
        // Map C# float (Single) to Avro double to align with ksqlDB AVRO limitations
        if (t == typeof(float)) return "\"double\"";
        if (t == typeof(double)) return "\"double\"";
        if (t == typeof(bool)) return "\"boolean\"";
        if (t == typeof(string)) return "[ \"null\", \"string\" ]";
        if (t == typeof(byte[])) return "\"bytes\"";
        if (t == typeof(decimal))
        {
            var precision = DecimalPrecisionConfig.ResolvePrecision(decAttr?.Precision, prop);
            var scale = DecimalPrecisionConfig.ResolveScale(decAttr?.Scale, prop);
            return $"{{ \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": {precision}, \"scale\": {scale} }}";
        }
        if (t == typeof(DateTime)) return "{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }";
        if (t == typeof(Guid)) return "\"string\"";
        return "\"string\"";
    }

    private static Type GenerateSpecificRecordType(Type pocoType, string schemaJson, string cacheKey)
    {
        var props = pocoType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var propTypes = new Type[props.Length];
        for (int i = 0; i < props.Length; i++)
        {
            var ptype = props[i].PropertyType;
            if (ptype == typeof(decimal))
            {
                propTypes[i] = typeof(AvroDecimal);
            }
            else if (Nullable.GetUnderlyingType(ptype) == typeof(decimal))
            {
                propTypes[i] = typeof(Nullable<>).MakeGenericType(typeof(AvroDecimal));
            }
            else if (ptype == typeof(Guid))
                propTypes[i] = typeof(string);
            else if (ptype == typeof(float))
                propTypes[i] = typeof(double);
            else
                propTypes[i] = props[i].PropertyType;
        }

        var fullName = cacheKey + "Avro";

        var typeBuilder = _moduleBuilder.DefineType(
            fullName,
            TypeAttributes.Public | TypeAttributes.Class);
        typeBuilder.AddInterfaceImplementation(typeof(ISpecificRecord));

        var schemaField = typeBuilder.DefineField("_SCHEMA", typeof(Schema), FieldAttributes.Private | FieldAttributes.Static | FieldAttributes.InitOnly);

        var cctor = typeBuilder.DefineConstructor(MethodAttributes.Static | MethodAttributes.Private, CallingConventions.Standard, Type.EmptyTypes);
        var ilCctor = cctor.GetILGenerator();
        ilCctor.Emit(OpCodes.Ldstr, schemaJson);
        ilCctor.Emit(OpCodes.Call, typeof(Schema).GetMethod("Parse", new[] { typeof(string) })!);
        ilCctor.Emit(OpCodes.Stsfld, schemaField);
        ilCctor.Emit(OpCodes.Ret);

        // public parameterless constructor
        typeBuilder.DefineDefaultConstructor(MethodAttributes.Public);

        var schemaProp = typeBuilder.DefineProperty("Schema", PropertyAttributes.None, typeof(Schema), null);
        var getSchema = typeBuilder.DefineMethod("get_Schema", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig | MethodAttributes.SpecialName, typeof(Schema), Type.EmptyTypes);
        var ilGetSchema = getSchema.GetILGenerator();
        ilGetSchema.Emit(OpCodes.Ldsfld, schemaField);
        ilGetSchema.Emit(OpCodes.Ret);
        schemaProp.SetGetMethod(getSchema);
        typeBuilder.DefineMethodOverride(getSchema, typeof(ISpecificRecord).GetProperty("Schema")!.GetGetMethod()!);

        var fields = new FieldBuilder[props.Length];
        for (int i = 0; i < props.Length; i++)
        {
            var p = props[i];
            var pt = propTypes[i];
            fields[i] = typeBuilder.DefineField("_" + p.Name, pt, FieldAttributes.Private);
            var propBuilder = typeBuilder.DefineProperty(p.Name, PropertyAttributes.None, pt, null);
            var getMethod = typeBuilder.DefineMethod("get_" + p.Name, MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName, pt, Type.EmptyTypes);
            var ilGet = getMethod.GetILGenerator();
            ilGet.Emit(OpCodes.Ldarg_0);
            ilGet.Emit(OpCodes.Ldfld, fields[i]);
            ilGet.Emit(OpCodes.Ret);
            var setMethod = typeBuilder.DefineMethod("set_" + p.Name, MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName, null, new[] { pt });
            var ilSet = setMethod.GetILGenerator();
            ilSet.Emit(OpCodes.Ldarg_0);
            ilSet.Emit(OpCodes.Ldarg_1);
            ilSet.Emit(OpCodes.Stfld, fields[i]);
            ilSet.Emit(OpCodes.Ret);
            propBuilder.SetGetMethod(getMethod);
            propBuilder.SetSetMethod(setMethod);
        }

        var getRecord = typeBuilder.DefineMethod("Get", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig, typeof(object), new[] { typeof(int) });
        var ilGetRecord = getRecord.GetILGenerator();
        var labels = new Label[props.Length];
        for (int i = 0; i < props.Length; i++)
            labels[i] = ilGetRecord.DefineLabel();
        var defaultLabel = ilGetRecord.DefineLabel();
        var endLabel = ilGetRecord.DefineLabel();

        ilGetRecord.Emit(OpCodes.Ldarg_1);
        ilGetRecord.Emit(OpCodes.Switch, labels);
        // use long branch to avoid range issues when many fields exist
        ilGetRecord.Emit(OpCodes.Br, defaultLabel);

        for (int i = 0; i < props.Length; i++)
        {
            ilGetRecord.MarkLabel(labels[i]);
            ilGetRecord.Emit(OpCodes.Ldarg_0);
            ilGetRecord.Emit(OpCodes.Ldfld, fields[i]);
            if (propTypes[i].IsValueType)
                ilGetRecord.Emit(OpCodes.Box, propTypes[i]);
            ilGetRecord.Emit(OpCodes.Br, endLabel);
        }

        ilGetRecord.MarkLabel(defaultLabel);
        ilGetRecord.Emit(OpCodes.Ldstr, "Bad index {0}");
        ilGetRecord.Emit(OpCodes.Ldarg_1);
        ilGetRecord.Emit(OpCodes.Box, typeof(int));
        ilGetRecord.Emit(OpCodes.Call, typeof(string).GetMethod("Format", new[] { typeof(string), typeof(object) })!);
        ilGetRecord.Emit(OpCodes.Newobj, typeof(AvroRuntimeException).GetConstructor(new[] { typeof(string) })!);
        ilGetRecord.Emit(OpCodes.Throw);

        ilGetRecord.MarkLabel(endLabel);
        ilGetRecord.Emit(OpCodes.Ret);

        typeBuilder.DefineMethodOverride(getRecord, typeof(ISpecificRecord).GetMethod("Get")!);

        var putRecord = typeBuilder.DefineMethod("Put", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig, typeof(void), new[] { typeof(int), typeof(object) });
        var ilPutRecord = putRecord.GetILGenerator();
        var putLabels = new Label[props.Length];
        for (int i = 0; i < props.Length; i++)
            putLabels[i] = ilPutRecord.DefineLabel();
        var putDefault = ilPutRecord.DefineLabel();
        var putEnd = ilPutRecord.DefineLabel();

        ilPutRecord.Emit(OpCodes.Ldarg_1);
        ilPutRecord.Emit(OpCodes.Switch, putLabels);
        // use long branch to avoid short branch limits
        ilPutRecord.Emit(OpCodes.Br, putDefault);

        for (int i = 0; i < props.Length; i++)
        {
            ilPutRecord.MarkLabel(putLabels[i]);
            ilPutRecord.Emit(OpCodes.Ldarg_0);
            ilPutRecord.Emit(OpCodes.Ldarg_2);
            if (propTypes[i].IsValueType)
                ilPutRecord.Emit(OpCodes.Unbox_Any, propTypes[i]);
            else
                ilPutRecord.Emit(OpCodes.Castclass, propTypes[i]);
            ilPutRecord.Emit(OpCodes.Stfld, fields[i]);
            ilPutRecord.Emit(OpCodes.Br, putEnd);
        }

        ilPutRecord.MarkLabel(putDefault);
        ilPutRecord.Emit(OpCodes.Ldstr, "Bad index {0}");
        ilPutRecord.Emit(OpCodes.Ldarg_1);
        ilPutRecord.Emit(OpCodes.Box, typeof(int));
        ilPutRecord.Emit(OpCodes.Call, typeof(string).GetMethod("Format", new[] { typeof(string), typeof(object) })!);
        ilPutRecord.Emit(OpCodes.Newobj, typeof(AvroRuntimeException).GetConstructor(new[] { typeof(string) })!);
        ilPutRecord.Emit(OpCodes.Throw);

        ilPutRecord.MarkLabel(putEnd);
        ilPutRecord.Emit(OpCodes.Ret);

        typeBuilder.DefineMethodOverride(putRecord, typeof(ISpecificRecord).GetMethod("Put")!);

        return typeBuilder.CreateType()!;
    }
}
