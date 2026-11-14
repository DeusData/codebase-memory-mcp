Public Interface ICustomerStore
    Sub Save(name As String)
End Interface

Public Class CustomerService
    Implements ICustomerStore

    Private lastName As String

    Public Sub Save(name As String) Implements ICustomerStore.Save
        If Validate(name) Then
            lastName = name
        End If
    End Sub

    Public Function Describe() As String
        Return "Customer: " & lastName
    End Function

    Private Function Validate(name As String) As Boolean
        Return Not String.IsNullOrEmpty(name)
    End Function

    Public Class Snapshot
        Public Sub Capture()
        End Sub
    End Class
End Class
